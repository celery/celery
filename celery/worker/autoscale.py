# -*- coding: utf-8 -*-
"""
    celery.worker.autoscale
    ~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the internal thread responsible
    for growing and shrinking the pool according to the
    current autoscale settings.

    The autoscale thread is only enabled if autoscale
    has been enabled on the command line.

"""
from __future__ import absolute_import
from __future__ import with_statement

import os
import threading

from functools import partial
from time import sleep, time

from celery.utils.log import get_logger
from celery.utils.threads import bgThread

from . import state
from .bootsteps import StartStopComponent
from .hub import DummyLock

logger = get_logger(__name__)
debug, info, error = logger.debug, logger.info, logger.error

AUTOSCALE_KEEPALIVE = int(os.environ.get('AUTOSCALE_KEEPALIVE', 30))


class WorkerComponent(StartStopComponent):
    name = 'worker.autoscaler'
    requires = ('pool', )

    def __init__(self, w, **kwargs):
        self.enabled = w.autoscale
        w.autoscaler = None

    def create_threaded(self, w):
        scaler = w.autoscaler = self.instantiate(
            w.autoscaler_cls,
            w.pool, w.max_concurrency, w.min_concurrency,
        )
        return scaler

    def on_poll_init(self, scaler, hub):
        hub.on_task.append(scaler.maybe_scale)
        hub.timer.apply_interval(scaler.keepalive * 1000.0, scaler.maybe_scale)

    def create_ev(self, w):
        scaler = w.autoscaler = self.instantiate(
            w.autoscaler_cls,
            w.pool, w.max_concurrency, w.min_concurrency,
            mutex=DummyLock(),
        )
        w.hub.on_init.append(partial(self.on_poll_init, scaler))

    def create(self, w):
        return (self.create_ev if w.use_eventloop
                else self.create_threaded)(w)


class Autoscaler(bgThread):

    def __init__(self, pool, max_concurrency,
                 min_concurrency=0, keepalive=AUTOSCALE_KEEPALIVE, mutex=None):
        super(Autoscaler, self).__init__()
        self.pool = pool
        self.mutex = mutex or threading.Lock()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self._last_action = None

        assert self.keepalive, 'cannot scale down too fast.'

    def body(self):
        with self.mutex:
            self.maybe_scale()
        sleep(1.0)

    def _maybe_scale(self):
        procs = self.processes
        cur = min(self.qty, self.max_concurrency)
        if cur > procs:
            self.scale_up(cur - procs)
            return True
        elif cur < procs:
            self.scale_down((procs - cur) - self.min_concurrency)
            return True

    def maybe_scale(self):
        if self._maybe_scale():
            self.pool.maintain_pool()

    def update(self, max=None, min=None):
        with self.mutex:
            if max is not None:
                if max < self.max_concurrency:
                    self._shrink(self.processes - max)
                self.max_concurrency = max
            if min is not None:
                if min > self.min_concurrency:
                    self._grow(min - self.min_concurrency)
                self.min_concurrency = min
            return self.max_concurrency, self.min_concurrency

    def force_scale_up(self, n):
        with self.mutex:
            new = self.processes + n
            if new > self.max_concurrency:
                self.max_concurrency = new
            self.min_concurrency += 1
            self._grow(n)

    def force_scale_down(self, n):
        with self.mutex:
            new = self.processes - n
            if new < self.min_concurrency:
                self.min_concurrency = max(new, 0)
            self._shrink(min(n, self.processes))

    def scale_up(self, n):
        self._last_action = time()
        return self._grow(n)

    def scale_down(self, n):
        if n and self._last_action and (
                time() - self._last_action > self.keepalive):
            self._last_action = time()
            return self._shrink(n)

    def _grow(self, n):
        info('Scaling up %s processes.', n)
        self.pool.grow(n)

    def _shrink(self, n):
        info('Scaling down %s processes.', n)
        try:
            self.pool.shrink(n)
        except ValueError:
            debug("Autoscaler won't scale down: all processes busy.")
        except Exception, exc:
            error('Autoscaler: scale_down: %r', exc, exc_info=True)

    def info(self):
        return {'max': self.max_concurrency,
                'min': self.min_concurrency,
                'current': self.processes,
                'qty': self.qty}

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool.num_processes

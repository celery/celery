# -*- coding: utf-8 -*-
"""
    celery.worker.autoscale
    ~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the internal thread responsible
    for growing and shrinking the pool according to the
    current autoscale settings.

    The autoscale thread is only enabled if autoscale
    has been enabled on the command line.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import sys
import threading
import traceback

from time import sleep, time

from . import state
from ..abstract import StartStopComponent
from ..utils.threads import bgThread


class WorkerComponent(StartStopComponent):
    name = "worker.autoscaler"
    requires = ("pool", )

    def __init__(self, w, **kwargs):
        self.enabled = w.autoscale
        w.autoscaler = None

    def create(self, w):
        scaler = w.autoscaler = self.instantiate(w.autoscaler_cls, w.pool,
                                    max_concurrency=w.max_concurrency,
                                    min_concurrency=w.min_concurrency,
                                    logger=w.logger)
        return scaler


class Autoscaler(bgThread):

    def __init__(self, pool, max_concurrency, min_concurrency=0,
            keepalive=30, logger=None):
        super(Autoscaler, self).__init__()
        self.pool = pool
        self.mutex = threading.Lock()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self.logger = logger
        self._last_action = None

        assert self.keepalive, "can't scale down too fast."

    def body(self):
        with self.mutex:
            current = min(self.qty, self.max_concurrency)
            if current > self.processes:
                self.scale_up(current - self.processes)
            elif current < self.processes:
                self.scale_down(
                    (self.processes - current) - self.min_concurrency)
        sleep(1.0)
    scale = body  # XXX compat

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
                self.min_concurrency = new
            self._shrink(n)

    def scale_up(self, n):
        self._last_action = time()
        return self._grow(n)

    def _grow(self, n):
        self.logger.info("Scaling up %s processes.", n)
        self.pool.grow(n)

    def _shrink(self, n):
        self.logger.info("Scaling down %s processes.", n)
        try:
            self.pool.shrink(n)
        except ValueError:
            self.logger.debug(
                "Autoscaler won't scale down: all processes busy.")
        except Exception, exc:
            self.logger.error("Autoscaler: scale_down: %r\n%r",
                                exc, traceback.format_stack(),
                                exc_info=sys.exc_info())

    def scale_down(self, n):
        if not self._last_action or not n:
            return
        if time() - self._last_action > self.keepalive:
            self._last_action = time()
            self._shrink(n)

    def info(self):
        return {"max": self.max_concurrency,
                "min": self.min_concurrency,
                "current": self.processes,
                "qty": self.qty}

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool.num_processes

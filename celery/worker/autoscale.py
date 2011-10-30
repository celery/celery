# -*- coding: utf-8 -*-
"""
    celery.worker.autoscale
    ~~~~~~~~~~~~~~~~~~~~~~~

    This module implements the internal thread responsible
    for growing and shrinking the pool according to the
    current autoscale settings.

    The autoscale thread is only enabled if autoscale
    has been enabled on the command line.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import os
import sys
import threading
import traceback

from time import sleep, time

from . import state


class Autoscaler(threading.Thread):

    def __init__(self, pool, max_concurrency, min_concurrency=0,
            keepalive=30, logger=None):
        threading.Thread.__init__(self)
        self.pool = pool
        self.mutex = threading.Lock()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self.logger = logger
        self._last_action = None
        self._is_shutdown = threading.Event()
        self._is_stopped = threading.Event()
        self.setDaemon(True)
        self.setName(self.__class__.__name__)

        assert self.keepalive, "can't scale down too fast."

    def scale(self):
        with self.mutex:
            current = min(self.qty, self.max_concurrency)
            if current > self.processes:
                self.scale_up(current - self.processes)
            elif current < self.processes:
                self.scale_down(
                    (self.processes - current) - self.min_concurrency)

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

    def run(self):
        while not self._is_shutdown.isSet():
            try:
                self.scale()
                sleep(1.0)
            except Exception, exc:
                self.logger.error("Thread Autoscaler crashed: %r", exc,
                                  exc_info=sys.exc_info())
                os._exit(1)
        self._is_stopped.set()

    def stop(self):
        self._is_shutdown.set()
        self._is_stopped.wait()
        if self.isAlive():
            self.join(1e10)

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

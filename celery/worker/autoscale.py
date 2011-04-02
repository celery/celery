import os
import sys
import threading
import traceback

from time import sleep, time

from celery.worker import state


class Autoscaler(threading.Thread):

    def __init__(self, pool, max_concurrency, min_concurrency=0,
            keepalive=30, logger=None):
        threading.Thread.__init__(self)
        self.pool = pool
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self.logger = logger
        self._last_action = None
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)
        self.setName(self.__class__.__name__)

        assert self.keepalive, "can't scale down too fast."

    def scale(self):
        current = min(self.qty, self.max_concurrency)
        if current > self.processes:
            self.scale_up(current - self.processes)
        elif current < self.processes:
            self.scale_down((self.processes - current) - self.min_concurrency)
        sleep(1.0)

    def scale_up(self, n):
        self.logger.info("Scaling up %s processes." % (n, ))
        self._last_action = time()
        return self.pool.grow(n)

    def scale_down(self, n):
        if not self._last_action or not n:
            return
        if time() - self._last_action > self.keepalive:
            self.logger.info("Scaling down %s processes." % (n, ))
            self._last_action = time()
            try:
                self.pool.shrink(n)
            except ValueError:
                self.logger.debug(
                    "Autoscaler won't scale down: all processes busy.")
            except Exception, exc:
                self.logger.error("Autoscaler: scale_down: %r\n%r" % (
                                    exc, traceback.format_stack()),
                                  exc_info=sys.exc_info())

    def run(self):
        while not self._shutdown.isSet():
            try:
                self.scale()
            except Exception, exc:
                self.logger.error("Thread Autoscaler crashed: %r" % (exc, ),
                                  exc_info=sys.exc_info())
                os._exit(1)
        self._stopped.set()

    def stop(self):
        self._shutdown.set()
        self._stopped.wait()
        if self.isAlive():
            self.join(1e10)

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool._pool._processes

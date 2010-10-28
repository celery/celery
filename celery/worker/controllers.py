"""

Worker Controller Threads

"""
import logging
import sys
import threading
import traceback

from time import sleep, time
from Queue import Empty

from celery.app import app_or_default
from celery.utils.compat import log_with_extra
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
            except Exception, exc:
                traceback.print_stack()
                self.logger.error("Autoscaler: scale_down: %r" % (exc, ))

    def run(self):
        while not self._shutdown.isSet():
            self.scale()
        self._stopped.set()

    def stop(self):
        self._shutdown.set()
        self._stopped.wait()
        self.join(1e100)

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool._pool._processes


class Mediator(threading.Thread):
    """Thread continuously moving tasks in the ready queue to the pool."""

    #: The task queue, a :class:`~Queue.Queue` instance.
    ready_queue = None

    #: Callback called when a task is obtained.
    callback = None

    def __init__(self, ready_queue, callback, logger=None, app=None):
        threading.Thread.__init__(self)
        self.app = app_or_default(app)
        self.logger = logger or self.app.log.get_default_logger()
        self.ready_queue = ready_queue
        self.callback = callback
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)
        self.setName(self.__class__.__name__)

    def move(self):
        try:
            task = self.ready_queue.get(timeout=1.0)
        except Empty:
            return

        if task.revoked():
            return

        self.logger.debug(
            "Mediator: Running callback for task: %s[%s]" % (
                task.task_name, task.task_id))

        try:
            self.callback(task)
        except Exception, exc:
            log_with_extra(self.logger, logging.ERROR,
                           "Mediator callback raised exception %r\n%s" % (
                               exc, traceback.format_exc()),
                           exc_info=sys.exc_info(),
                           extra={"data": {"hostname": task.hostname,
                                           "id": task.task_id,
                                           "name": task.task_name}})

    def run(self):
        """Move tasks forver or until :meth:`stop` is called."""
        while not self._shutdown.isSet():
            self.move()
        self._stopped.set()

    def stop(self):
        """Gracefully shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait()
        self.join(1e100)

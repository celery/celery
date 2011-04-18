import os
import sys
import threading
import traceback

from Queue import Empty

from celery.app import app_or_default


class Mediator(threading.Thread):
    """Thread continuously moving tasks from the ready queue onto the pool."""

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
            self.logger.error("Mediator callback raised exception %r\n%s" % (
                                exc, traceback.format_exc()),
                              exc_info=sys.exc_info(),
                              extra={"data": {"id": task.task_id,
                                              "name": task.task_name,
                                              "hostname": task.hostname}})

    def run(self):
        """Move tasks until :meth:`stop` is called."""
        while not self._shutdown.isSet():
            try:
                self.move()
            except Exception, exc:
                self.logger.error("Mediator crash: %r" % (exc, ),
                    exc_info=sys.exc_info())
                # exiting by normal means does not work here, so force exit.
                os._exit(1)
        self._stopped.set()

    def stop(self):
        """Gracefully shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait()
        self.join(1e10)

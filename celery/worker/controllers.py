"""

Worker Controller Threads

"""
import logging
import sys
import threading
import traceback
from Queue import Empty as QueueEmpty

from celery.app import app_or_default
from celery.utils.compat import log_with_extra


class Mediator(threading.Thread):
    """Thread continuously sending tasks in the queue to the pool.

    .. attribute:: ready_queue

        The task queue, a :class:`Queue.Queue` instance.

    .. attribute:: callback

        The callback used to process tasks retrieved from the
        :attr:`ready_queue`.

    """

    def __init__(self, ready_queue, callback, logger=None,
            app=None):
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
            # This blocks until there's a message in the queue.
            task = self.ready_queue.get(timeout=1.0)
        except QueueEmpty:
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
        while not self._shutdown.isSet():
            self.move()
        self._stopped.set()                 # indicate that we are stopped

    def stop(self):
        """Gracefully shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait()                # block until this thread is done
        self.join(1e100)

"""

Worker Controller Threads

"""
import time
import threading
from Queue import Empty as QueueEmpty

from celery import log


class Mediator(threading.Thread):
    """Thread continuously sending tasks in the queue to the pool.

    .. attribute:: ready_queue

        The task queue, a :class:`Queue.Queue` instance.

    .. attribute:: callback

        The callback used to process tasks retrieved from the
        :attr:`ready_queue`.

    """

    def __init__(self, ready_queue, callback, logger=None):
        threading.Thread.__init__(self)
        self.logger = logger or log.get_default_logger()
        self.ready_queue = ready_queue
        self.callback = callback
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)

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
        self.callback(task) # execute

    def run(self):
        while not self._shutdown.isSet():
            self.move()
        self._stopped.set() # indicate that we are stopped

    def stop(self):
        """Gracefully shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done
        self.join(1e100)

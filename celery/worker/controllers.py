"""

Worker Controller Threads

"""
import time
import threading
from Queue import Empty as QueueEmpty
from datetime import datetime

from celery.log import get_default_logger
from celery.worker.revoke import revoked


class BackgroundThread(threading.Thread):
    """Thread running an infinite loop which for every iteration
    calls its :meth:`on_iteration` method.

    This also implements graceful shutdown of the thread by providing
    the :meth:`stop` method.

    """
    is_infinite = True

    def __init__(self):
        super(BackgroundThread, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)

    def run(self):
        """This is the body of the thread.

        To start the thread use :meth:`start` instead.

        """
        self.on_start()

        while self.is_infinite:
            if self._shutdown.isSet():
                break
            self.on_iteration()
        self._stopped.set() # indicate that we are stopped

    def on_start(self):
        """This handler is run at thread start, just before the infinite
        loop."""
        pass

    def on_iteration(self):
        """This is the method called for every iteration and must be
        implemented by every subclass of :class:`BackgroundThread`."""
        raise NotImplementedError(
                "InfiniteThreads must implement on_iteration")

    def on_stop(self):
        """This handler is run when the thread is shutdown."""
        pass

    def stop(self):
        """Gracefully shutdown the thread."""
        self.on_stop()
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done


class Mediator(BackgroundThread):
    """Thread continuously sending tasks in the queue to the pool.

    .. attribute:: ready_queue

        The task queue, a :class:`Queue.Queue` instance.

    .. attribute:: callback

        The callback used to process tasks retrieved from the
        :attr:`ready_queue`.

    """

    def __init__(self, ready_queue, callback):
        super(Mediator, self).__init__()
        self.ready_queue = ready_queue
        self.callback = callback

    def on_iteration(self):
        """Get tasks from bucket queue and apply the task callback."""
        logger = get_default_logger()
        try:
            # This blocks until there's a message in the queue.
            task = self.ready_queue.get(timeout=1)
        except QueueEmpty:
            time.sleep(1)
        else:
            if task.task_id in revoked: # task revoked
                logger.warn("Mediator: Skipping revoked task: %s[%s]" % (
                    task.task_name, task.task_id))
                return

            logger.debug("Mediator: Running callback for task: %s[%s]" % (
                task.task_name, task.task_id))
            self.callback(task) # execute


class ScheduleController(BackgroundThread):
    """Schedules tasks with an ETA by moving them to the bucket queue."""

    def __init__(self, eta_schedule):
        super(ScheduleController, self).__init__()
        self._scheduler = iter(eta_schedule)
        self.iterations = 0

    def on_iteration(self):
        """Wake-up scheduler"""
        logger = get_default_logger()
        delay = self._scheduler.next()
        debug_log = True
        if delay is None:
            delay = 1
            if self.iterations == 10:
                self.iterations = 0
            else:
                debug_log = False
                self.iterations += 1
        if debug_log:
            logger.debug("ScheduleController: Scheduler wake-up")
            logger.debug(
                "ScheduleController: Next wake-up eta %s seconds..." % (
                    delay))
        time.sleep(delay)

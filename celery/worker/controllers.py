"""

Worker Controller Threads

"""
from celery.backends import default_periodic_status_backend
from Queue import Empty as QueueEmpty
from datetime import datetime
from multiprocessing import get_logger
import threading
import time


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

    .. attribute:: bucket_queue

        The task queue, a :class:`Queue.Queue` instance.

    .. attribute:: callback

        The callback used to process tasks retrieved from the
        :attr:`bucket_queue`.

    """

    def __init__(self, bucket_queue, callback):
        super(Mediator, self).__init__()
        self.bucket_queue = bucket_queue
        self.callback = callback

    def on_iteration(self):
        logger = get_logger()
        try:
            logger.debug("Mediator: Trying to get message from bucket_queue")
            # This blocks until there's a message in the queue.
            task = self.bucket_queue.get(timeout=1)
        except QueueEmpty:
            logger.debug("Mediator: Bucket queue is empty.")
            pass
        else:
            logger.debug("Mediator: Running callback for task: %s[%s]" % (
                task.task_name, task.task_id))
            self.callback(task)


class PeriodicWorkController(BackgroundThread):
    """A thread that continuously checks if there are
    :class:`celery.task.PeriodicTask` tasks waiting for execution,
    and executes them. It also finds tasks in the hold queue that is
    ready for execution and moves them to the bucket queue.

    (Tasks in the hold queue are tasks waiting for retry, or with an
    ``eta``/``countdown``.)

    """

    def __init__(self, bucket_queue, hold_queue):
        super(PeriodicWorkController, self).__init__()
        self.hold_queue = hold_queue
        self.bucket_queue = bucket_queue

    def on_start(self):
        """Do backend-specific periodic task initialization."""
        default_periodic_status_backend.init_periodic_tasks()

    def on_iteration(self):
        logger = get_logger()
        logger.debug("PeriodicWorkController: Running periodic tasks...")
        self.run_periodic_tasks()
        logger.debug("PeriodicWorkController: Processing hold queue...")
        self.process_hold_queue()
        logger.debug("PeriodicWorkController: Going to sleep...")
        time.sleep(1)

    def run_periodic_tasks(self):
        logger = get_logger()
        applied = default_periodic_status_backend.run_periodic_tasks()
        for task, task_id in applied:
            logger.debug(
                "PeriodicWorkController: Periodic task %s applied (%s)" % (
                    task.name, task_id))

    def process_hold_queue(self):
        """Finds paused tasks that are ready for execution and move
        them to the :attr:`bucket_queue`."""
        logger = get_logger()
        try:
            logger.debug(
                "PeriodicWorkController: Getting next task from hold queue..")
            task, eta = self.hold_queue.get_nowait()
        except QueueEmpty:
            logger.debug("PeriodicWorkController: Hold queue is empty")
            return
        if datetime.now() >= eta:
            logger.debug(
                "PeriodicWorkController: Time to run %s[%s] (%s)..." % (
                    task.task_name, task.task_id, eta))
            self.bucket_queue.put(task)
        else:
            logger.debug(
                "PeriodicWorkController: ETA not ready for %s[%s] (%s)..." % (
                    task.task_name, task.task_id, eta))
            self.hold_queue.put((task, eta))

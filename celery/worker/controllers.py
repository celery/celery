"""

Worker Controller Threads

"""
from celery.backends import default_periodic_status_backend
from Queue import Empty as QueueEmpty
from datetime import datetime
import threading
import time


class InfinityThread(threading.Thread):
    """Thread running an infinite loop which for every iteration
    calls its :meth:`on_iteration` method.

    This also implements graceful shutdown of the thread by providing
    the :meth:`stop` method.

    """

    def __init__(self):
        super(InfinityThread, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()

    def run(self):
        """This is the body of the thread.
        
        To start the thread use :meth:`start` instead.
        
        """
        while True:
            if self._shutdown.isSet():
                break
            self.on_iteration()
        self._stopped.set() # indicate that we are stopped

    def on_iteration(self):
        """This is the method called for every iteration and must be 
        implemented by every subclass of :class:`InfinityThread`."""
        raise NotImplementedError(
                "InfiniteThreads must implement on_iteration")
    
    def stop(self):
        """Gracefully shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done


class Mediator(InfinityThread):
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
        try:
            # This blocks until there's a message in the queue.
            task = self.bucket_queue.get(timeout=1)
        except QueueEmpty:
            pass
        else:
            self.callback(task)


class PeriodicWorkController(InfinityThread):
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

    def on_iteration(self):
        self.run_periodic_tasks()
        self.process_hold_queue()
        time.sleep(1)

    def run_periodic_tasks(self):
        default_periodic_status_backend.run_periodic_tasks()

    def process_hold_queue(self):
        """Finds paused tasks that are ready for execution and move
        them to the :attr:`bucket_queue`."""
        try:
            task, eta = self.hold_queue.get_nowait()
        except QueueEmpty:
            return
        if datetime.now() >= eta:
            self.bucket_queue.put(task)
        else:
            self.hold_queue.put((task, eta))

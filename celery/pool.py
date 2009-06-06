import multiprocessing
import itertools
import threading
import uuid
import time
import os

from multiprocessing.pool import RUN as POOL_STATE_RUN
from celery.timer import TimeoutTimer, TimeoutError
from celery.conf import REAP_TIMEOUT
from celery.datastructures import ExceptionInfo


class TaskPool(object):
    """Pool of running child processes, which starts waiting for the
    processes to finish when the queue limit has been reached.

    :param limit: see :attr:`limit` attribute.

    :param logger: see :attr:`logger` attribute.

    :param done_msg: see :attr:`done_msg` attribute.


    .. attribute:: limit

        The number of processes that can run simultaneously until
        we start collecting results.

    .. attribute:: logger

        The logger used to print the :attr:`done_msg`.

    .. attribute:: done_msg

        Message logged when a tasks result has been collected.
        The message is logged with loglevel :const:`logging.INFO`.

    """

    def __init__(self, limit, reap_timeout=None):
        self.limit = limit
        self.reap_timeout = reap_timeout
        self._process_counter = itertools.count(1)
        self._processed_total = 0

    def run(self):
        self._start()

    def _start(self):
        self._processes = {}
        self._pool = multiprocessing.Pool(processes=self.limit)

    def _terminate_and_restart(self):
        try:
            self._pool.terminate()
        except OSError:
            pass
        self._start()

    def _restart(self):
        self.logger.info("Closing and restarting the pool...")
        self._pool.close()
        timeout_thread = threading.Timer(30.0, self._terminate_and_restart)
        timeout_thread.start()
        self._pool.join()
        timeout_thread.cancel()
        self._start()

    def _pool_is_running(self):
        return self._pool._state == POOL_STATE_RUN

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, meta=None):
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []
        meta = meta or {}
        id = str(uuid.uuid4())

        if not self._pool_is_running():
            self._start()

        self._processed_total = self._process_counter.next()

        on_return = lambda r: self.on_return(r, id, callbacks, errbacks, meta)

        result = self._pool.apply_async(target, args, kwargs,
                                           callback=on_return)

        self.add(result, callbacks, errbacks, id, meta)

        return result

    def on_return(self, ret_val, id, callbacks, errbacks, meta):
        try:
            del(self._processes[id])
        except KeyError:
            pass
        else:
            self.on_ready(ret_val, callbacks, errbacks, meta)

    def add(self, result, callbacks, errbacks, id, meta):
        """Add a process to the queue.

        If the queue is full, it will wait for the first task to finish,
        collects its result and remove it from the queue, so it's ready
        to accept new processes.

        :param result: A :class:`multiprocessing.AsyncResult` instance, as
            returned by :meth:`multiprocessing.Pool.apply_async`.

        :option callbacks: List of callbacks to execute if the task was
            successful. Must have the function signature:
                ``mycallback(result, meta)``

        :option errbacks: List of errbacks to execute if the task raised
            and exception. Must have the function signature:
                ``myerrback(exc, meta)``.

        :option id: Explicitly set the id for this task.
st
        """

        self._processes[id] = [result, callbacks, errbacks, meta]

        if self.full():
            self.wait_for_result()

    def full(self):
        return len(self._processes.values()) >= self.limit

    def wait_for_result(self):
        """Waits for the first process in the pool to finish.

        This operation is blocking.

        """
        while True:
            if self.reap():
                break

    def reap(self):
        self.logger.debug("Reaping processes...")
        processes_reaped = 0
        for process_no, entry in enumerate(self._processes.items()):
            id, process_info = entry
            result, callbacks, errbacks, meta = process_info
            try:
                ret_value = result.get(timeout=0.3)
            except multiprocessing.TimeoutError:
                continue
            else:
                self.on_return(ret_value, id, callbacks, errbacks, meta)
                processes_reaped += 1
        return processes_reaped

    def get_worker_pids(self):
        """Returns the process id's of all the pool workers.

        :rtype: list

        """
        return [process.pid for process in self._pool._pool]

    def on_ready(self, ret_value, callbacks, errbacks, meta):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            for errback in errbacks:
                errback(ret_value, meta)
        else:
            for callback in callbacks:
                callback(ret_value, meta)

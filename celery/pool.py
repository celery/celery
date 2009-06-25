"""

Process Pools.

"""
import multiprocessing
import itertools
import threading
import uuid

from multiprocessing.pool import RUN as POOL_STATE_RUN
from celery.datastructures import ExceptionInfo
from functools import partial as curry


class TaskPool(object):
    """Pool of running child processes, which starts waiting for the
    processes to finish when the queue limit has been reached.

    :param limit: see :attr:`limit` attribute.
    :param logger: see :attr:`logger` attribute.


    .. attribute:: limit

        The number of processes that can run simultaneously until
        we start collecting results.

    .. attribute:: logger

        The logger used for debugging.

    """

    def __init__(self, limit, logger=None):
        self.limit = limit
        self.logger = logger or multiprocessing.get_logger()
        self._process_counter = itertools.count(1)
        self._processed_total = 0
        self._pool = None
        self._processes = None

    def start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._processes = {}
        self._pool = multiprocessing.Pool(processes=self.limit)

    def stop(self):
        """Terminate the pool."""
        self._pool.terminate()
        self._processes = {}
        self._pool = None

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, on_acknowledge=None, meta=None):
        """Equivalent of the :func:``apply`` built-in function.

        All ``callbacks`` and ``errbacks`` should complete immediately since
        otherwise the thread which handles the result will get blocked.

        """
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []
        meta = meta or {}
        tid = str(uuid.uuid4())

        self._processed_total = self._process_counter.next()

        on_return = curry(self.on_return, tid, callbacks, errbacks, meta)

        if self.full():
            self.wait_for_result()

        result = self._pool.apply_async(target, args, kwargs,
                                        callback=on_return)
        if on_acknowledge:
            on_acknowledge()
        
        self._processes[tid] = [result, callbacks, errbacks, meta]

        return result

    def on_return(self, tid, callbacks, errbacks, meta, ret_value):
        """What to do when the process returns."""
        try:
            del(self._processes[tid])
        except KeyError:
            pass
        else:
            self.on_ready(callbacks, errbacks, meta, ret_value)

    def full(self):
        """Is the pool full?

        :returns: ``True`` if the maximum number of concurrent processes
            has been reached.

        """
        return len(self._processes.values()) >= self.limit

    def wait_for_result(self):
        """Waits for the first process in the pool to finish.

        This operation is blocking.

        """
        while True:
            if self.reap():
                break

    def reap(self):
        """Reap finished tasks."""
        self.logger.debug("Reaping processes...")
        processes_reaped = 0
        for process_no, entry in enumerate(self._processes.items()):
            tid, process_info = entry
            result, callbacks, errbacks, meta = process_info
            try:
                ret_value = result.get(timeout=0.3)
            except multiprocessing.TimeoutError:
                continue
            else:
                self.on_return(tid, callbacks, errbacks, meta, ret_value)
                processes_reaped += 1
        return processes_reaped

    def get_worker_pids(self):
        """Returns the process id's of all the pool workers."""
        return [process.pid for process in self._pool._pool]

    def on_ready(self, callbacks, errbacks, meta, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, KeyboardInterrupt) or \
                    isinstance(ret_value.exception, SystemExit):
                raise ret_value.exception
            for errback in errbacks:
                errback(ret_value, meta)
        else:
            for callback in callbacks:
                callback(ret_value, meta)

"""

Process Pools.

"""
import multiprocessing
import itertools
import threading
import uuid

from multiprocessing.pool import RUN as POOL_STATE_RUN
from celery.datastructures import ExceptionInfo


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

    def __init__(self, limit, reap_timeout=None, logger=None):
        self.limit = limit
        self.logger = logger or multiprocessing.get_logger()
        self.reap_timeout = reap_timeout
        self._process_counter = itertools.count(1)
        self._processed_total = 0
        self._pool = None
        self._processes = None

    def run(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._start()

    def _start(self):
        """INTERNAL: Starts the pool. Used by :meth:`run`."""
        self._processes = {}
        self._pool = multiprocessing.Pool(processes=self.limit)

    def terminate(self):
        """Terminate the pool."""
        self._pool.terminate()

    def _terminate_and_restart(self):
        """INTERNAL: Terminate and restart the pool."""
        try:
            self.terminate()
        except OSError:
            pass
        self._start()

    def _restart(self):
        """INTERNAL: Close and restart the pool."""
        self.logger.info("Closing and restarting the pool...")
        self._pool.close()
        timeout_thread = threading.Timer(30.0, self._terminate_and_restart)
        timeout_thread.start()
        self._pool.join()
        timeout_thread.cancel()
        self._start()

    def _pool_is_running(self):
        """Check if the pool is in the run state.

        :returns: ``True`` if the pool is running.

        """
        return self._pool._state == POOL_STATE_RUN

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

        if not self._pool_is_running():
            self._start()

        self._processed_total = self._process_counter.next()

        on_return = lambda r: self.on_return(r, tid, callbacks, errbacks, meta)


        if self.full():
            self.wait_for_result()
        result = self._pool.apply_async(target, args, kwargs,
                                           callback=on_return)
        if on_acknowledge:
            on_acknowledge()
        self.add(result, callbacks, errbacks, tid, meta)

        return result

    def on_return(self, ret_val, tid, callbacks, errbacks, meta):
        """What to do when the process returns."""
        try:
            del(self._processes[tid])
        except KeyError:
            pass
        else:
            self.on_ready(ret_val, callbacks, errbacks, meta)

    def add(self, result, callbacks, errbacks, tid, meta):
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

        :option tid: The tid for this task (unqiue pool id).

        """

        self._processes[tid] = [result, callbacks, errbacks, meta]

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
                self.on_return(ret_value, tid, callbacks, errbacks, meta)
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
            if isinstance(ret_value.exception, KeyboardInterrupt) or \
                    isinstance(ret_value.exception, SystemExit):
                self.terminate()
                raise ret_value.exception
            for errback in errbacks:
                errback(ret_value, meta)
        else:
            for callback in callbacks:
                callback(ret_value, meta)

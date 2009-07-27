"""

Process Pools.

"""
import os
import time
import errno
import multiprocessing

from multiprocessing.pool import Pool, worker
from celery.datastructures import ExceptionInfo
from celery.utils import gen_unique_id
from functools import partial as curry


def pid_is_dead(pid):
    try:
        return os.kill(pid, 0)
    except OSError, err:
        if err.errno == errno.ESRCH:
            return True # No such process.
        elif err.errno == errno.EPERM:
            return False # Operation not permitted.
        else:
            raise


def reap_process(pid):
    if pid_is_dead(pid):
        return True

    try:
        is_dead, _ = os.waitpid(pid, os.WNOHANG)
    except OSError, err:
        if err.errno == errno.ECHILD:
            return False # No child processes.
        raise
    return is_dead


class DynamicPool(Pool):
    """Version of :class:`multiprocessing.Pool` that can dynamically grow
    in size."""

    def __init__(self, processes=None, initializer=None, initargs=()):
        super(DynamicPool, self).__init__(processes=processes,
                                          initializer=initializer,
                                          initargs=initargs)
        self._initializer = initializer
        self._initargs = initargs

    def add_worker(self):
        """Add another worker to the pool."""
        w = self.Process(target=worker,
                         args=(self._inqueue, self._outqueue,
                               self._initializer, self._initargs))
        self._pool.append(w)
        w.name = w.name.replace("Process", "PoolWorker")
        w.daemon = True
        w.start()

    def grow(self, size=1):
        """Add ``size`` new workers to the pool."""
        map(self.add_worker, range(size))

    def is_dead(self, process):
        # First try to see if the process is actually running,
        # and reap zombie proceses while we're at it.
        if reap_process(process.pid):
            return True
    
        # Then try to ping the process using its pipe.
        try:
            proc_is_alive = process.is_alive()
        except OSError:
            return True
        else:
            return not proc_is_alive

    def replace_dead_workers(self):
        dead_processes = filter(self.is_dead, self._pool)

        if dead_processes:
            dead_pids = [process.pid for process in dead_processes]
            self._pool = [process for process in self._pool
                            if process not in dead_pids]
            self.grow(len(dead_processes))
        
        return dead_processes


class TaskPool(object):
    """Process Pool for processing tasks in parallel.

    :param limit: see :attr:`limit` attribute.
    :param logger: see :attr:`logger` attribute.


    .. attribute:: limit

        The number of processes that can run simultaneously.

    .. attribute:: logger

        The logger used for debugging.

    """

    def __init__(self, limit, logger=None):
        self.limit = limit
        self.logger = logger or multiprocessing.get_logger()
        self._pool = None

    def start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._pool = DynamicPool(processes=self.limit)

    def stop(self):
        """Terminate the pool."""
        self._pool.terminate()
        self._pool = None

    def replace_dead_workers(self):
        self.logger.debug("TaskPool: Finding dead pool processes...")
        dead = self._pool.replace_dead_workers()
        if dead:
            self.logger.info(
                "TaskPool: Replaced %s dead pool workers..." % (
                    len(dead)))

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, on_ack=None, meta=None):
        """Equivalent of the :func:``apply`` built-in function.

        All ``callbacks`` and ``errbacks`` should complete immediately since
        otherwise the thread which handles the result will get blocked.

        """
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []
        meta = meta or {}

        on_return = curry(self.on_return, callbacks, errbacks,
                          on_ack, meta)


        self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        self.replace_dead_workers()

        return self._pool.apply_async(target, args, kwargs,
                                        callback=on_return)


    def on_return(self, callbacks, errbacks, on_ack, meta, ret_value):
        """What to do when the process returns."""

        # Acknowledge the task as being processed.
        if on_ack:
            on_ack()

        self.on_ready(callbacks, errbacks, meta, ret_value)

    def on_ready(self, callbacks, errbacks, meta, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            for errback in errbacks:
                errback(ret_value, meta)
        else:
            for callback in callbacks:
                callback(ret_value, meta)

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
from operator import isNumberType


def pid_is_dead(pid):
    """Check if a process is not running by PID.
   
    :rtype bool:

    """
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
    """Reap process if the process is a zombie.
   
    :returns: ``True`` if process was reaped or is not running,
        ``False`` otherwise.

    """
    if pid_is_dead(pid):
        return True

    try:
        is_dead, _ = os.waitpid(pid, os.WNOHANG)
    except OSError, err:
        if err.errno == errno.ECHILD:
            return False # No child processes.
        raise
    return is_dead

    
def process_is_dead(process):
    """Check if process is not running anymore.

    First it finds out if the process is running by sending
    signal 0. Then if the process is a child process, and is running
    it finds out if it's a zombie process and reaps it.
    If the process is running and is not a zombie it tries to send
    a ping through the process pipe.

    :param process: A :class:`multiprocessing.Process` instance.

    :returns: ``True`` if the process is not running, ``False`` otherwise.

    """

    # Try to see if the process is actually running,
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


class DynamicPool(Pool):
    """Version of :class:`multiprocessing.Pool` that can dynamically grow
    in size."""

    def __init__(self, processes=None, initializer=None, initargs=()):

        if processes is None:
            try:
                processes = cpu_count()
            except NotImplementedError:
                processes = 1

        super(DynamicPool, self).__init__(processes=processes,
                                          initializer=initializer,
                                          initargs=initargs)
        self._initializer = initializer
        self._initargs = initargs
        self._size = processes
        self.logger = multiprocessing.get_logger()

    def _my_cleanup(self):
        from multiprocessing.process import _current_process
        for p in list(_current_process._children):
            discard = False
            try:
                status = p._popen.poll()
            except OSError:
                discard = True
            else:
                if status is not None:
                    discard = True
            if discard:
                _current_process._children.discard(p)

    def add_worker(self):
        """Add another worker to the pool."""
        self._my_cleanup()
        w = self.Process(target=worker,
                         args=(self._inqueue, self._outqueue,
                               self._initializer, self._initargs))
        w.name = w.name.replace("Process", "PoolWorker")
        w.daemon = True
        w.start()
        self._pool.append(w)
        self.logger.debug(
            "DynamicPool: Started pool worker %s (PID: %s, Poolsize: %d)" %(
                w.name, w.pid, len(self._pool)))

    def grow(self, size=1):
        """Add workers to the pool.
       
        :keyword size: Number of workers to add (default: 1)
        
        """
        [self.add_worker() for i in range(size)]

    def _is_dead(self, process):
        """Try to find out if the process is dead.

        :rtype bool:

        """
        if process_is_dead(process):
            self.logger.info("DynamicPool: Found dead process (PID: %s)" % (
                process.pid))
            return True
        return False

    def _bring_out_the_dead(self):
        """Sort out dead process from pool.

        :returns: Tuple of two lists, the first list with dead processes,
            the second with active processes.

        """
        dead, alive = [], []
        for process in self._pool:
            if process and process.pid and isNumberType(process.pid):
                dest = dead if self._is_dead(process) else alive
                dest.append(process)
        return dead, alive 

    def replace_dead_workers(self):
        """Replace dead workers in the pool by spawning new ones.
        
        :returns: number of dead processes replaced, or ``None`` if all
            processes are alive and running.

        """
        dead, alive = self._bring_out_the_dead()
        if dead:
            dead_count = len(dead)
            self._pool = alive
            self.grow(self._size if dead_count > self._size else dead_count)
            return dead_count


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
        dead_count = self._pool.replace_dead_workers()
        if dead_count:
            self.logger.info(
                "TaskPool: Replaced %d dead pool workers..." % (
                    dead_count))

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

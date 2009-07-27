"""

Process Pools.

"""
import time
import multiprocessing

from multiprocessing.pool import Pool, worker
from celery.datastructures import ExceptionInfo
from celery.utils import gen_unique_id
from functools import partial as curry

MAX_RESTART_FREQ = 10
MAX_RESTART_FREQ_TIME = 60


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
        map(self._add_worker, range(size))

    def get_worker_pids(self):
        """Returns the process id's of all the pool workers."""
        return [process.pid for process in self.processes]

    def is_alive(self, process):
        try:
            proc_is_alive = process.is_alive()
        except OSError:
            return False
        else:
            return proc_is_alive

    def replace_dead_workers(self):
        dead = [process for process in self.processes
                            if not self.is_alive(process)]
        if dead:
            dead_pids = [process.pid for process in dead]
            self._pool = [process for process in self._pool
                            if process.pid not in dead_pids]
            self.grow(len(dead))
        
        return dead

    @property
    def processes(self):
        return self._pool


class PoolSupervisor(object):
    """Supervisor implementing the "one_for_one" strategy.

    :param target: See :attr:`target`.
    :param max_restart_freq: See :attr:`max_restart_freq`.
    :param max_restart_freq_time: See :attr:`max_restart_freq_time`.

    .. attribute:: target

        The target pool to supervise.

    .. attribute:: max_restart_freq

        Limit the number of restarts which can occur in a given time interval.

        The max restart frequency is the number of restarts that can occur
        within the interval :attr:`max_restart_freq_time`.

        The restart mechanism prevents situations where the process repeatedly
        dies for the same reason. If this happens both the process and the
        supervisor is terminated.

    .. attribute:: max_restart_freq_time

        See :attr:`max_restart_freq`.

    """

    def __init__(self, target, logger=None,
            max_restart_freq=MAX_RESTART_FREQ,
            max_restart_freq_time=MAX_RESTART_FREQ_TIME):
        self.logger = logger or multiprocessing.get_logger()
        self.target = target
        self.max_restart_freq = max_restart_freq * len(target.processes)
        self.max_restart_freq_time = max_restart_freq_time
        self.restart_frame_time = None
        self.restarts_in_frame = 0

    def restart_freq_exceeded(self):
        if not self.restart_frame_time:
            self.restart_frame_time = time.time()
            return False
        time_exceeded = time.time() > self.max_restart_frame_time + \
                self.max_restart_freq_time
        if time_exceeded:
            if self.restarts_in_frame >= self.max_restart_freq:
                return True
            self.restart_frame_time = time.time()
        return False
          
    def supervise(self):
        self.logger.debug("PoolSupervisor: Finding dead worker processes...")
        dead = self.target.replace_dead_workers()
        if dead:
            self.logger.info(
                "PoolSupervisor: Replaced %s dead pool workers..." % (
                    len(dead)))
            self.restarts_in_frame += len(dead)
            if self.restart_freq_exceeded():
                raise MaxRestartsExceededError(
                    "Pool supervisor: Max restart frequencey exceeded.")
                    

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
        self._supervisor = None

    def start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._pool = DynamicPool(processes=self.limit)
        self._supervisor = PoolSupervisor(self._pool)

    def stop(self):
        """Terminate the pool."""
        self._pool.terminate()
        self._pool = None

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

        self._supervisor.supervise()

        self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        return self._pool.apply_async(target, args, kwargs,
                                        callback=on_return)


    def on_return(self, callbacks, errbacks, on_ack, meta, ret_value):
        """What to do when the process returns."""

        # Acknowledge the task as being processed.
        if on_ack:
            on_ack()

        self.on_ready(callbacks, errbacks, meta, ret_value)

    def get_worker_pids(self):
        """Returns the process id's of all the pool workers."""
        return [process.pid for process in self._pool._pool]

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

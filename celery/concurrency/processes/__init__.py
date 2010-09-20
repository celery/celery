"""

Process Pools.

"""
from time import sleep, time

from celery import log
from celery.datastructures import ExceptionInfo
from celery.utils.functional import partial

from celery.concurrency.processes.pool import Pool, RUN


def pingback(i):
    return i


class TaskPool(object):
    """Process Pool for processing tasks in parallel.

    :param limit: see :attr:`limit`.
    :param logger: see :attr:`logger`.


    .. attribute:: limit

        The number of processes that can run simultaneously.

    .. attribute:: logger

        The logger used for debugging.

    """
    Pool = Pool

    def __init__(self, limit, logger=None, initializer=None,
            maxtasksperchild=None, timeout=None, soft_timeout=None,
            putlocks=True, initargs=None):
        self.limit = limit
        self.logger = logger or log.get_default_logger()
        self.initializer = initializer
        self.initargs = initargs
        self.maxtasksperchild = maxtasksperchild
        self.timeout = timeout
        self.soft_timeout = soft_timeout
        self.putlocks = putlocks
        self._pool = None

    def start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._pool = self.Pool(processes=self.limit,
                               initializer=self.initializer,
                               initargs=self.initargs,
                               timeout=self.timeout,
                               soft_timeout=self.soft_timeout,
                               maxtasksperchild=self.maxtasksperchild)

    def stop(self):
        """Gracefully stop the pool."""
        if self._pool is not None and self._pool._state == RUN:
            self._pool.close()
            self._pool.join()
            self._pool = None

    def terminate(self):
        """Force terminate the pool."""
        if self._pool is not None:
            self._pool.terminate()
            self._pool = None

    def diagnose(self, timeout=None):
        pids = set(worker.pid for worker in self._pool._pool)
        seen = set()
        results = {}
        time_start = time()

        def callback(i):
            for pid in results[i].worker_pids():
                seen.add(pid)

        i = 0
        while pids ^ seen:
            if time() - time_start > timeout:
                break
            results[i] = self._pool.apply_async(pingback,
                                                args=(i, ),
                                                callback=callback)
            sleep(0.1)
            i += 1

        return {"active": list(seen),
                "waiting": list(pids ^ seen),
                "iterations": i}

    def apply_async(self, target, args=None, kwargs=None, callbacks=None,
            errbacks=None, accept_callback=None, timeout_callback=None,
            **compat):
        """Equivalent of the :func:``apply`` built-in function.

        All ``callbacks`` and ``errbacks`` should complete immediately since
        otherwise the thread which handles the result will get blocked.

        """
        args = args or []
        kwargs = kwargs or {}
        callbacks = callbacks or []
        errbacks = errbacks or []

        on_ready = partial(self.on_ready, callbacks, errbacks)
        on_worker_error = partial(self.on_worker_error, errbacks)

        self.logger.debug("TaskPool: Apply %s (args:%s kwargs:%s)" % (
            target, args, kwargs))

        return self._pool.apply_async(target, args, kwargs,
                                      callback=on_ready,
                                      accept_callback=accept_callback,
                                      timeout_callback=timeout_callback,
                                      error_callback=on_worker_error,
                                      waitforslot=self.putlocks)

    def on_worker_error(self, errbacks, exc):
        einfo = ExceptionInfo((exc.__class__, exc, None))
        [errback(einfo) for errback in errbacks]

    def on_ready(self, callbacks, errbacks, ret_value):
        """What to do when a worker task is ready and its return value has
        been collected."""

        if isinstance(ret_value, ExceptionInfo):
            if isinstance(ret_value.exception, (
                    SystemExit, KeyboardInterrupt)):
                raise ret_value.exception
            [errback(ret_value) for errback in errbacks]
        else:
            [callback(ret_value) for callback in callbacks]

    @property
    def info(self):
        return {"max-concurrency": self.limit,
                "processes": [p.pid for p in self._pool._pool],
                "max-tasks-per-child": self.maxtasksperchild,
                "put-guarded-by-semaphore": self.putlocks,
                "timeouts": (self.soft_timeout, self.timeout)}

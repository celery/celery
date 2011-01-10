"""

Process Pools.

"""
import os
import signal as _signal

from celery.concurrency.base import BasePool
from celery.concurrency.processes.pool import Pool, RUN


class TaskPool(BasePool):
    """Process Pool for processing tasks in parallel.

    :param processes: see :attr:`processes`.
    :param logger: see :attr:`logger`.


    .. attribute:: limit

        The number of processes that can run simultaneously.

    .. attribute:: logger

        The logger used for debugging.

    """
    Pool = Pool

    def on_start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._pool = self.Pool(processes=self.limit, **self.options)
        self.on_apply = self._pool.apply_async

    def on_stop(self):
        """Gracefully stop the pool."""
        if self._pool is not None and self._pool._state == RUN:
            self._pool.close()
            self._pool.join()
            self._pool = None

    def on_terminate(self):
        """Force terminate the pool."""
        if self._pool is not None:
            self._pool.terminate()
            self._pool = None

    def terminate_job(self, pid, signal=None):
        os.kill(pid, signal or _signal.SIGTERM)

    def grow(self, n=1):
        return self._pool.grow(n)

    def shrink(self, n=1):
        return self._pool.shrink(n)

    def _get_info(self):
        return {"max-concurrency": self.limit,
                "processes": [p.pid for p in self._pool._pool],
                "max-tasks-per-child": self._pool._maxtasksperchild,
                "put-guarded-by-semaphore": self.putlocks,
                "timeouts": (self._pool.soft_timeout, self._pool.timeout)}

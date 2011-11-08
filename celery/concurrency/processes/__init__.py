# -*- coding: utf-8 -*-
from __future__ import absolute_import

import platform
import signal as _signal

from ..base import BasePool
from .pool import Pool, RUN

if platform.system() == "Windows":  # pragma: no cover
    # On Windows os.kill calls TerminateProcess which cannot be
    # handled by # any process, so this is needed to terminate the task
    # *and its children* (if any).
    from ._win import kill_processtree as _kill  # noqa
else:
    from os import kill as _kill                 # noqa


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
        _kill(pid, signal or _signal.SIGTERM)

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

    @property
    def num_processes(self):
        return self._pool._processes

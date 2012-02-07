# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import platform
import signal as _signal


from ... import platforms
from ... import signals
from ...app import app_or_default
from ..base import BasePool
from .pool import Pool, RUN

if platform.system() == "Windows":  # pragma: no cover
    # On Windows os.kill calls TerminateProcess which cannot be
    # handled by # any process, so this is needed to terminate the task
    # *and its children* (if any).
    from ._win import kill_processtree as _kill  # noqa
else:
    from os import kill as _kill                 # noqa

#: List of signals to reset when a child process starts.
WORKER_SIGRESET = frozenset(["SIGTERM",
                             "SIGHUP",
                             "SIGTTIN",
                             "SIGTTOU",
                             "SIGUSR1"])

#: List of signals to ignore when a child process starts.
WORKER_SIGIGNORE = frozenset(["SIGINT"])


def process_initializer(app, hostname):
    """Initializes the process so it can be used to process tasks."""
    app = app_or_default(app)
    app.set_current()
    platforms.signals.reset(*WORKER_SIGRESET)
    platforms.signals.ignore(*WORKER_SIGIGNORE)
    platforms.set_mp_process_title("celeryd", hostname=hostname)
    # This is for Windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    app.log.setup(int(os.environ.get("CELERY_LOG_LEVEL", 0)),
                  os.environ.get("CELERY_LOG_FILE") or None,
                  bool(os.environ.get("CELERY_LOG_REDIRECT", False)),
                  str(os.environ.get("CELERY_LOG_REDIRECT_LEVEL")))
    app.loader.init_worker()
    app.loader.init_worker_process()
    signals.worker_process_init.send(sender=None)


class TaskPool(BasePool):
    """Multiprocessing Pool implementation."""
    Pool = Pool

    requires_mediator = True

    def on_start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        self._pool = self.Pool(processes=self.limit,
                               initializer=process_initializer,
                               **self.options)
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

    def restart(self):
        self._pool.restart()

    def _get_info(self):
        return {"max-concurrency": self.limit,
                "processes": [p.pid for p in self._pool._pool],
                "max-tasks-per-child": self._pool._maxtasksperchild,
                "put-guarded-by-semaphore": self.putlocks,
                "timeouts": (self._pool.soft_timeout, self._pool.timeout)}

    @property
    def num_processes(self):
        return self._pool._processes

# -*- coding: utf-8 -*-
"""
    celery.concurrency.prefork
    ~~~~~~~~~~~~~~~~~~~~~~~~~~

    Pool implementation using :mod:`multiprocessing`.

"""
from __future__ import absolute_import

import os

from billiard import forking_enable
from billiard.pool import RUN, CLOSE, Pool as BlockingPool

from celery import platforms
from celery import signals
from celery._state import set_default_app
from celery.app import trace
from celery.concurrency.base import BasePool
from celery.five import items
from celery.utils.log import get_logger

from .asynpool import AsynPool

__all__ = ['TaskPool', 'process_initializer', 'process_destructor']

#: List of signals to reset when a child process starts.
WORKER_SIGRESET = frozenset(['SIGTERM',
                             'SIGHUP',
                             'SIGTTIN',
                             'SIGTTOU',
                             'SIGUSR1'])

#: List of signals to ignore when a child process starts.
WORKER_SIGIGNORE = frozenset(['SIGINT'])

MAXTASKS_NO_BILLIARD = """\
    maxtasksperchild enabled but billiard C extension not installed!
    This may lead to a deadlock, so please install the billiard C extension.
"""

logger = get_logger(__name__)
warning, debug = logger.warning, logger.debug


def process_initializer(app, hostname):
    """Pool child process initializer.

    This will initialize a child pool process to ensure the correct
    app instance is used and things like
    logging works.

    """
    platforms.signals.reset(*WORKER_SIGRESET)
    platforms.signals.ignore(*WORKER_SIGIGNORE)
    platforms.set_mp_process_title('celeryd', hostname=hostname)
    # This is for Windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    app.loader.init_worker()
    app.loader.init_worker_process()
    app.log.setup(int(os.environ.get('CELERY_LOG_LEVEL', 0) or 0),
                  os.environ.get('CELERY_LOG_FILE') or None,
                  bool(os.environ.get('CELERY_LOG_REDIRECT', False)),
                  str(os.environ.get('CELERY_LOG_REDIRECT_LEVEL')))
    if os.environ.get('FORKED_BY_MULTIPROCESSING'):
        # pool did execv after fork
        trace.setup_worker_optimizations(app)
    else:
        app.set_current()
        set_default_app(app)
        app.finalize()
        trace._tasks = app._tasks  # enables fast_trace_task optimization.
    # rebuild execution handler for all tasks.
    from celery.app.trace import build_tracer
    for name, task in items(app.tasks):
        task.__trace__ = build_tracer(name, task, app.loader, hostname,
                                      app=app)
    signals.worker_process_init.send(sender=None)


def process_destructor(pid, exitcode):
    """Pool child process destructor

    Dispatch the :signal:`worker_process_shutdown` signal.

    """
    signals.worker_process_shutdown.send(
        sender=None, pid=pid, exitcode=exitcode,
    )


class TaskPool(BasePool):
    """Multiprocessing Pool implementation."""
    Pool = AsynPool
    BlockingPool = BlockingPool

    uses_semaphore = True
    write_stats = None

    def on_start(self):
        """Run the task pool.

        Will pre-fork all workers so they're ready to accept tasks.

        """
        if self.options.get('maxtasksperchild'):
            try:
                from billiard.connection import Connection
                Connection.send_offset
            except (ImportError, AttributeError):
                # billiard C extension not installed
                warning(MAXTASKS_NO_BILLIARD)

        forking_enable(self.forking_enable)
        Pool = (self.BlockingPool if self.options.get('threads', True)
                else self.Pool)
        P = self._pool = Pool(processes=self.limit,
                              initializer=process_initializer,
                              on_process_exit=process_destructor,
                              synack=False,
                              **self.options)

        # Create proxy methods
        self.on_apply = P.apply_async
        self.maintain_pool = P.maintain_pool
        self.terminate_job = P.terminate_job
        self.grow = P.grow
        self.shrink = P.shrink
        self.flush = getattr(P, 'flush', None)  # FIXME add to billiard
        self.restart = P.restart

    def did_start_ok(self):
        return self._pool.did_start_ok()

    def register_with_event_loop(self, loop):
        try:
            reg = self._pool.register_with_event_loop
        except AttributeError:
            return
        return reg(loop)

    def on_stop(self):
        """Gracefully stop the pool."""
        if self._pool is not None and self._pool._state in (RUN, CLOSE):
            self._pool.close()
            self._pool.join()
            self._pool = None

    def on_terminate(self):
        """Force terminate the pool."""
        if self._pool is not None:
            self._pool.terminate()
            self._pool = None

    def on_close(self):
        if self._pool is not None and self._pool._state == RUN:
            self._pool.close()

    def _get_info(self):
        return {
            'max-concurrency': self.limit,
            'processes': [p.pid for p in self._pool._pool],
            'max-tasks-per-child': self._pool._maxtasksperchild or 'N/A',
            'put-guarded-by-semaphore': self.putlocks,
            'timeouts': (self._pool.soft_timeout or 0,
                         self._pool.timeout or 0),
            'writes': self._pool.human_write_stats(),
        }

    @property
    def num_processes(self):
        return self._pool._processes

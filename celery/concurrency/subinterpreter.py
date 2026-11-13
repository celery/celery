"""Subinterpreter execution pool.

Tasks run in a fixed set of isolated interpreters (:pep:`734`, Python 3.14+),
each with its own GIL, driven by threads of the main interpreter.  Only the
trace function and the message data cross into a subinterpreter: the
callbacks run in the main interpreter, which owns the broker connection.

A running subinterpreter cannot be interrupted, so time limits are not
supported, and a task that is revoked with ``terminate=True`` or running at
cold shutdown runs to completion.
"""
from __future__ import annotations

import os
import queue
import sys
import time
from concurrent import interpreters
from concurrent.futures import ThreadPoolExecutor
from typing import TYPE_CHECKING, Any, Callable

from billiard.einfo import ExceptionInfo
from billiard.exceptions import WorkerLostError

from celery import signals
from celery._state import _set_task_join_will_block
from celery.app import base as _app_base
from celery.app import trace
from celery.utils.log import get_logger

from .base import BasePool
from .thread import ApplyResult as _ThreadApplyResult

__all__ = ('TaskPool', 'subinterpreter_initializer')

if TYPE_CHECKING:
    from typing import TypedDict

    from celery import Celery

    PoolInfo = TypedDict('PoolInfo', {'max-concurrency': int, 'interpreters': int})

    # `TargetFunction` should be a Protocol that represents fast_trace_task and
    # trace_task_ret.
    TargetFunction = Callable[..., Any]

logger = get_logger(__name__)


def subinterpreter_initializer(app: Celery, hostname: str) -> None:
    """Pool subinterpreter initializer.

    A subinterpreter starts as a fresh Python, so this mirrors what
    :func:`~celery.concurrency.prefork.process_initializer` does for a
    spawned child process, minus the per-process parts (signals, process
    title, death signal) that belong to the main interpreter.
    """
    _set_task_join_will_block(True)
    # Have the task modules imported by init_worker() register their tasks
    # with this (unpickled) app, as for a spawned prefork child.
    _app_base.USING_EXECV = True
    app.loader.init_worker()
    app.loader.init_worker_process()
    app.log.setup(int(os.environ.get('CELERY_LOG_LEVEL', 0) or 0),
                  os.environ.get('CELERY_LOG_FILE') or None,
                  bool(os.environ.get('CELERY_LOG_REDIRECT', False)),
                  str(os.environ.get('CELERY_LOG_REDIRECT_LEVEL')),
                  hostname=hostname)
    trace.setup_worker_optimizations(app, hostname)
    # rebuild execution handler for all tasks.
    from celery.app.trace import build_tracer
    for name, task in app.tasks.items():
        task.__trace__ = build_tracer(name, task, app.loader, hostname,
                                      app=app)
    from celery.worker import state as worker_state
    worker_state.reset_state()
    signals.worker_process_init.send(sender=None)


class ApplyResult(_ThreadApplyResult):
    def terminate(self, signal: int | None = None) -> None:
        # Only a job still waiting for an interpreter can be stopped.
        self.f.cancel()


def _setup_interpreter(
    sys_path: list[str],
    initializer: Callable[..., None] | None,
    initargs: tuple[Any, ...],
) -> None:
    # Subinterpreters do not inherit changes the main interpreter made to
    # sys.path (e.g. the current directory added by ``celery -A``).
    sys.path[:] = sys_path
    if initializer is not None:
        initializer(*initargs)


class TaskPool(BasePool):
    """Subinterpreter Task Pool."""

    limit: int

    body_can_be_buffer = False
    signal_safe = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        if self.limit is None:
            self.limit = os.process_cpu_count() or 1
        initargs = self.options.get('initargs')
        self.initializer = subinterpreter_initializer if initargs else None
        self.initargs = initargs or ()
        self.executor = ThreadPoolExecutor(
            max_workers=self.limit, thread_name_prefix='celery-subinterp')
        self._interpreters: list[interpreters.Interpreter] = []
        self._idle: queue.SimpleQueue[interpreters.Interpreter] = (
            queue.SimpleQueue())

    def on_start(self) -> None:
        if self.options.get('timeout') or self.options.get('soft_timeout'):
            logger.warning(
                'Time limits are not supported by the subinterpreter pool '
                'and will be ignored.')
        # Create the interpreters up front, in parallel, so that a task
        # module (or C extension) that cannot be loaded in a subinterpreter
        # fails the worker at startup and not on the first task.
        try:
            for interp in self.executor.map(
                    lambda _: self._create_interpreter(), range(self.limit)):
                self._interpreters.append(interp)
                self._idle.put(interp)
        except interpreters.ExecutionFailed:
            logger.critical(
                'Could not set up a subinterpreter, see the error below. If '
                'an extension module does not support loading in a '
                'subinterpreter, use another pool.')
            raise

    def _create_interpreter(self) -> interpreters.Interpreter:
        interp = interpreters.create()
        try:
            interp.call(_setup_interpreter, list(sys.path),
                        self.initializer, self.initargs)
        except BaseException:
            interp.close()
            raise
        return interp

    def on_stop(self) -> None:
        # Wait for running tasks; queued ones were never accepted (so never
        # acked) and will be redelivered.
        self.executor.shutdown(cancel_futures=True)
        self._close_interpreters()
        super().on_stop()

    def on_terminate(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)
        self._close_interpreters()

    def _close_interpreters(self) -> None:
        while self._interpreters:
            interp = self._interpreters.pop()
            try:
                interp.close()
            except interpreters.InterpreterError as exc:
                # still running a task (terminate), cannot be interrupted.
                logger.warning('Could not close subinterpreter %s: %s',
                               interp.id, exc)

    def terminate_job(self, pid: int, signal: int | None = None) -> None:
        # Called to revoke with terminate=True and at cold shutdown, which
        # must not fail: the task is left to run to completion.
        logger.warning('The subinterpreter pool cannot terminate a running '
                       'task, it will run to completion.')

    def on_apply(
        self,
        target: TargetFunction,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        callback: Callable[..., Any] | None = None,
        accept_callback: Callable[..., Any] | None = None,
        error_callback: Callable[..., Any] | None = None,
        **_: Any
    ) -> ApplyResult:
        f = self.executor.submit(self._run, target, args or (), kwargs or {},
                                 callback, accept_callback, error_callback)
        return ApplyResult(f)

    def _run(
        self,
        target: TargetFunction,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        callback: Callable[..., Any] | None,
        accept_callback: Callable[..., Any] | None,
        error_callback: Callable[..., Any] | None,
    ) -> Any:
        interp = self._idle.get()
        try:
            if accept_callback:
                accept_callback(os.getpid(), time.monotonic())
            try:
                ret = interp.call(target, *args, **kwargs)
            except Exception as exc:
                # The trace functions report task errors in their return
                # value, so this is a failure to run the task at all (e.g. an
                # unregistered task or an unpicklable argument or result).
                try:
                    raise WorkerLostError(
                        f'Could not run task in subinterpreter: {exc}',
                    ) from exc
                except WorkerLostError:
                    einfo = ExceptionInfo()
                if error_callback is None:
                    raise
                error_callback(einfo)
                return None
        finally:
            self._idle.put(interp)
        if callback:
            callback(ret)
        return ret

    def _get_info(self) -> PoolInfo:
        info = super()._get_info()
        info.update({
            'max-concurrency': self.limit,
            'interpreters': len(self._interpreters),
        })
        return info

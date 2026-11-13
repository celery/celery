"""Subinterpreter execution pool.

Tasks run in a fixed set of isolated interpreters (:pep:`734`, Python 3.14+),
each with its own GIL, driven by threads of the main interpreter.  Only the
trace function and the message data cross into a subinterpreter: the
callbacks run in the main interpreter, which owns the broker connection.

A running subinterpreter cannot be interrupted: hard time limits are not
supported, and a task that is revoked with ``terminate=True``, running at
cold shutdown or over its soft time limit is only asked to stop, see
:func:`stop_requested`.
"""
from __future__ import annotations

import os
import queue
import sys
import threading
import time
from concurrent import interpreters
from concurrent.futures import Future, ThreadPoolExecutor, wait
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

__all__ = ('TaskPool', 'stop_requested', 'subinterpreter_initializer')

if TYPE_CHECKING:
    from typing import TypedDict

    from celery import Celery

    PoolInfo = TypedDict('PoolInfo', {
        'max-concurrency': int,
        'max-tasks-per-child': int | str,
        'interpreters': int,
    })

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


def stop_requested() -> bool:
    """Return whether the running task was asked to stop.

    A running subinterpreter cannot be interrupted, so the pool asks the task
    to stop instead: when it is revoked with ``terminate=True``, on cold
    shutdown, or when its soft time limit is exceeded.  Long running tasks
    can check this regularly and return early.

    Always false outside of a subinterpreter pool.
    """
    return _stop_queue is not None and not _stop_queue.empty()


#: In a pool subinterpreter, the queue the main interpreter writes to when
#: the running task should stop.
_stop_queue: interpreters.Queue | None = None


def _setup_interpreter(
    sys_path: list[str],
    stop_queue: interpreters.Queue,
    initializer: Callable[..., None] | None,
    initargs: tuple[Any, ...],
) -> None:
    global _stop_queue
    # Subinterpreters do not inherit changes the main interpreter made to
    # sys.path (e.g. the current directory added by ``celery -A``).
    sys.path[:] = sys_path
    _stop_queue = stop_queue
    if initializer is not None:
        initializer(*initargs)


def _shutdown_interpreter() -> None:
    signals.worker_process_shutdown.send(
        sender=None, pid=os.getpid(), exitcode=0,
    )


class _Interpreter:
    """A pool subinterpreter."""

    def __init__(self, interp: interpreters.Interpreter,
                 stop_queue: interpreters.Queue) -> None:
        self.interp = interp
        self.stop_queue = stop_queue
        self.tasks = 0

    def request_stop(self) -> None:
        self.stop_queue.put(True)

    def clear_stop(self) -> None:
        while True:
            try:
                self.stop_queue.get_nowait()
            except interpreters.QueueEmpty:
                break

    def close(self) -> None:
        try:
            self.interp.call(_shutdown_interpreter)
            self.interp.close()
        except interpreters.InterpreterError as exc:
            # still running a task (terminate), cannot be interrupted.
            logger.warning('Could not close subinterpreter %s: %s',
                           self.interp.id, exc)


class _Job:
    """Tracks the interpreter running a job, to ask it to stop."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.interp: _Interpreter | None = None
        self.stop = False

    def start(self, interp: _Interpreter) -> None:
        with self.lock:
            interp.clear_stop()  # left over from a previous job
            self.interp = interp
            if self.stop:
                interp.request_stop()

    def done(self) -> None:
        with self.lock:
            self.interp = None

    def request_stop(self) -> None:
        with self.lock:
            self.stop = True
            if self.interp is not None:
                self.interp.request_stop()


class ApplyResult(_ThreadApplyResult):
    def __init__(self, future: Future, job: _Job) -> None:
        super().__init__(future)
        self._job = job

    def terminate(self, signal: int | None = None) -> None:
        if not self.f.cancel():
            logger.warning(
                'The subinterpreter pool cannot terminate a running task, '
                'asked it to stop: it does if it checks stop_requested().')
            self._job.request_stop()


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
        self.max_tasks_per_child = self.options.get('maxtasksperchild')
        self.executor = ThreadPoolExecutor(
            max_workers=self.limit, thread_name_prefix='celery-subinterp')
        self._lock = threading.Lock()
        self._interpreters: list[_Interpreter] = []
        self._idle: queue.SimpleQueue[_Interpreter] = queue.SimpleQueue()
        # Requests only keep a weak reference to their ApplyResult, which
        # must stay alive until the job is done so they can terminate it.
        self._results: set[ApplyResult] = set()

    def on_start(self) -> None:
        if self.options.get('timeout'):
            logger.warning(
                'Hard time limits are not supported by the subinterpreter '
                'pool and will be ignored.')
        # Create the interpreters up front, in parallel, so that a task
        # module (or C extension) that cannot be loaded in a subinterpreter
        # fails the worker at startup and not on the first task.
        futures = [self.executor.submit(self._create_interpreter)
                   for _ in range(self.limit)]
        wait(futures)
        created = [f.result() for f in futures if f.exception() is None]
        errors = [f.exception() for f in futures if f.exception() is not None]
        if errors:
            for interp in created:
                interp.close()
            if isinstance(errors[0], interpreters.ExecutionFailed):
                logger.critical(
                    'Could not set up a subinterpreter, see the error below. '
                    'If an extension module does not support loading in a '
                    'subinterpreter, use another pool.')
            raise errors[0]
        with self._lock:
            self._interpreters.extend(created)
        for interp in created:
            self._idle.put(interp)

    def _create_interpreter(self) -> _Interpreter:
        interp = interpreters.create()
        stop_queue = interpreters.create_queue()
        try:
            interp.call(_setup_interpreter, list(sys.path), stop_queue,
                        self.initializer, self.initargs)
        except BaseException:
            interp.close()
            raise
        return _Interpreter(interp, stop_queue)

    def _replace_interpreter(self, old: _Interpreter) -> _Interpreter:
        try:
            new = self._create_interpreter()
        except Exception:
            logger.exception('Could not replace subinterpreter %s, keeping it',
                             old.interp.id)
            old.tasks = 0
            return old
        with self._lock:
            try:
                self._interpreters[self._interpreters.index(old)] = new
            except ValueError:  # the pool was stopped meanwhile.
                new, old = old, new
        old.close()
        return new

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
        with self._lock:
            interps, self._interpreters = self._interpreters, []
        for interp in interps:
            interp.close()

    def terminate_job(self, pid: int, signal: int | None = None) -> None:
        # Called to revoke with terminate=True and at cold shutdown, along
        # with ApplyResult.terminate() which asks the task to stop: all jobs
        # run in this process, the pid does not tell which one to stop.
        pass

    def on_apply(
        self,
        target: TargetFunction,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        callback: Callable[..., Any] | None = None,
        accept_callback: Callable[..., Any] | None = None,
        error_callback: Callable[..., Any] | None = None,
        soft_timeout: float | None = None,
        **_: Any
    ) -> ApplyResult:
        job = _Job()
        f = self.executor.submit(self._run, job, target, args or (),
                                 kwargs or {}, callback, accept_callback,
                                 error_callback, soft_timeout)
        result = ApplyResult(f, job)
        with self._lock:
            self._results.add(result)
        f.add_done_callback(lambda _: self._forget(result))
        return result

    def _forget(self, result: ApplyResult) -> None:
        with self._lock:
            self._results.discard(result)

    def _run(self, job: _Job, *args: Any) -> Any:
        interp = self._idle.get()
        try:
            return self._run_in(interp, job, *args)
        finally:
            interp.tasks += 1
            if (self.max_tasks_per_child and
                    interp.tasks >= self.max_tasks_per_child):
                interp = self._replace_interpreter(interp)
            self._idle.put(interp)

    def _run_in(
        self,
        interp: _Interpreter,
        job: _Job,
        target: TargetFunction,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        callback: Callable[..., Any] | None,
        accept_callback: Callable[..., Any] | None,
        error_callback: Callable[..., Any] | None,
        soft_timeout: float | None,
    ) -> Any:
        if accept_callback:
            accept_callback(os.getpid(), time.monotonic())
        timer = None
        if soft_timeout:
            timer = threading.Timer(soft_timeout, self._on_soft_timeout,
                                    (job, soft_timeout))
            timer.daemon = True
        job.start(interp)
        try:
            if timer is not None:
                timer.start()
            ret = interp.interp.call(target, *args, **kwargs)
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
            if timer is not None:
                timer.cancel()
            job.done()
        if callback:
            callback(ret)
        return ret

    @staticmethod
    def _on_soft_timeout(job: _Job, soft_timeout: float) -> None:
        logger.warning('Soft time limit (%ss) exceeded, asked the task to '
                       'stop: it does if it checks stop_requested().',
                       soft_timeout)
        job.request_stop()

    def _get_info(self) -> PoolInfo:
        info = super()._get_info()
        info.update({
            'max-concurrency': self.limit,
            'max-tasks-per-child': self.max_tasks_per_child or 'N/A',
            'interpreters': len(self._interpreters),
        })
        return info

"""Thread execution pool."""
from __future__ import annotations

import ctypes
import sys
import threading
from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import TYPE_CHECKING, Any, Callable

from celery.exceptions import Terminated
from celery.utils.log import get_logger
from celery.worker import state as worker_state

from .base import BasePool, apply_target

__all__ = ('TaskPool',)

logger = get_logger(__name__)

IS_PYPY = hasattr(sys, 'pypy_version_info')

if TYPE_CHECKING:
    from typing import TypedDict

    PoolInfo = TypedDict('PoolInfo', {'max-concurrency': int, 'threads': int})

    # `TargetFunction` should be a Protocol that represents fast_trace_task and
    # trace_task_ret.
    TargetFunction = Callable[..., Any]


class ApplyResult:
    def __init__(self, future: Future) -> None:
        self.f = future
        self.get = self.f.result

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)

    def terminate(self, signal: int | None = None) -> None:
        self.f.cancel()


class TaskPool(BasePool):
    """Thread Task Pool."""
    limit: int

    body_can_be_buffer = True
    signal_safe = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.executor = ThreadPoolExecutor(max_workers=self.limit)
        self._running: set[int] = set()
        self._mutex = threading.Lock()

    def terminate_job(self, pid: int, signal: int | None = None) -> None:
        """Raise :exc:`~celery.exceptions.Terminated` in the task's thread.

        CPython delivers it once that thread next runs Python bytecode,
        so a task blocked in a system call keeps running until the call
        returns.
        """
        # Locked so the thread can't take the next task mid-injection.
        with self._mutex:
            if pid not in self._running:
                return
            if IS_PYPY:  # pragma: no cover
                logger.warning('cannot terminate task thread %s on PyPy', pid)
                return

            affected = ctypes.pythonapi.PyThreadState_SetAsyncExc(
                ctypes.c_ulong(pid), ctypes.py_object(Terminated))
            if affected == 0:
                logger.warning('failed to terminate task thread %s (not found)', pid)
            elif affected > 1:  # pragma: no cover
                ctypes.pythonapi.PyThreadState_SetAsyncExc(
                    ctypes.c_ulong(pid), None)
                logger.warning('failed to terminate task thread %s (affected=%s)', pid, affected)

    def on_terminate(self) -> None:
        self.executor.shutdown(wait=False, cancel_futures=True)

    def on_stop(self) -> None:
        terminating = (worker_state.should_terminate is not None
                       and worker_state.should_terminate is not False)
        self.executor.shutdown(wait=not terminating, cancel_futures=True)
        super().on_stop()

    def on_apply(
        self,
        target: TargetFunction,
        args: tuple[Any, ...] | None = None,
        kwargs: dict[str, Any] | None = None,
        callback: Callable[..., Any] | None = None,
        accept_callback: Callable[..., Any] | None = None,
        **_: Any
    ) -> ApplyResult:
        def run() -> None:
            tid = threading.get_ident()
            with self._mutex:
                self._running.add(tid)
            try:
                apply_target(target, args, kwargs, callback, accept_callback,
                             pid=tid)
            finally:
                with self._mutex:
                    self._running.discard(tid)

        return ApplyResult(self.executor.submit(run))

    def _get_info(self) -> PoolInfo:
        info = super()._get_info()
        info.update({
            'max-concurrency': self.limit,
            'threads': len(self.executor._threads)
        })
        return info

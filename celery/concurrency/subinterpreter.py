"""Subinterpreter execution pool."""
from __future__ import annotations

from collections.abc import Callable
from concurrent.futures import Future, InterpreterPoolExecutor, wait
from typing import TYPE_CHECKING, Any

from .base import BasePool, apply_target

__all__ = ('TaskPool',)

if TYPE_CHECKING:
    from typing import TypedDict

    PoolInfo = TypedDict('PoolInfo', {'max-concurrency': int, 'interpreters': int })

    # `TargetFunction` should be a Protocol that represents fast_trace_task and
    # trace_task_ret.
    TargetFunction = Callable[..., Any]

class ApplyResult:
    def __init__(self, future: Future) -> None:
        self.f = future
        self.get = self.f.result

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)

class TaskPool(BasePool):
    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.executor = InterpreterPoolExecutor(max_workers=self.limit)

    def on_stop(self) -> None:
        self.executor.shutdown(cancel_futures=True)
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
        f = self.executor.submit(apply_target, target, args, kwargs,
                                 callback, accept_callback)
        return ApplyResult(f)

    def _get_info(self) -> PoolInfo:
        info = super()._get_info()
        info.update({
            'max-concurrency': self.limit,
            'interpreters': len(self.executor._threads),
        })
        return info

"""Thread execution pool."""
from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, wait
from typing import TYPE_CHECKING, Any, Callable

from celery.utils.log import get_logger

from .base import AsyncPoolShutdownMixin, BasePool, apply_target

__all__ = ('TaskPool',)

if TYPE_CHECKING:
    from typing import TypedDict

    PoolInfo = TypedDict('PoolInfo', {'max-concurrency': int, 'threads': int})

    # `TargetFunction` should be a Protocol that represents fast_trace_task and
    # trace_task_ret.
    TargetFunction = Callable[..., Any]

logger = get_logger(__name__)


class ApplyResult:
    def __init__(self, future: Future) -> None:
        self.f = future
        self.get = self.f.result

    def wait(self, timeout: float | None = None) -> None:
        wait([self.f], timeout)


class TaskPool(BasePool, AsyncPoolShutdownMixin):
    """Thread Task Pool."""
    limit: int

    body_can_be_buffer = True
    signal_safe = False

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.executor = ThreadPoolExecutor(max_workers=self.limit)

    def on_stop(self) -> None:
        if event_loop_started := self.start_timer_event_loop(pool_type="thread"):
            shutdown_event, timer_thread = event_loop_started

            try:
                self.executor.shutdown()
            finally:
                shutdown_event.set()
                timer_thread.join(timeout=1.0)

                if timer_thread.is_alive():
                    logger.warning(
                        "Timer thread in thread on_stop() did not terminate cleanly"
                    )

        else:
            self.executor.shutdown()

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
            'threads': len(self.executor._threads)
        })
        return info

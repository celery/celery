# -*- coding: utf-8 -*-
"""Pool Autoscaling.

This module implements the internal thread responsible
for growing and shrinking the pool according to the
current autoscale settings.

The autoscale thread is only enabled if
the :option:`celery worker --autoscale` option is used.
"""
import os
import threading
<<<<<<< HEAD
from time import sleep

=======
from time import monotonic, sleep
from typing import Mapping, Optional, Tuple
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu.async.semaphore import DummyLock
from celery import bootsteps
from celery.types import AutoscalerT, LoopT, PoolT, RequestT, WorkerT
from celery.utils.log import get_logger
from celery.utils.threads import bgThread
from . import state
from .components import Pool

__all__ = ('Autoscaler', 'WorkerComponent')

logger = get_logger(__name__)
debug, info, error = logger.debug, logger.info, logger.error

AUTOSCALE_KEEPALIVE = float(os.environ.get('AUTOSCALE_KEEPALIVE', 30))


class WorkerComponent(bootsteps.StartStopStep):
    """Bootstep that starts the autoscaler thread/timer in the worker."""

    label = 'Autoscaler'
    conditional = True
    requires = (Pool,)

    def __init__(self, w: WorkerT, **kwargs) -> None:
        self.enabled = w.autoscale
        w.autoscaler = None

    def create(self, w: WorkerT) -> Optional[AutoscalerT]:
        scaler = w.autoscaler = self.instantiate(
            w.autoscaler_cls,
            w.pool, w.max_concurrency, w.min_concurrency,
            worker=w, mutex=DummyLock() if w.use_eventloop else None,
        )
        return scaler if not w.use_eventloop else None

    def register_with_event_loop(self, w: WorkerT, hub: LoopT) -> None:
        w.consumer.on_task_message.add(w.autoscaler.maybe_scale)
        hub.call_repeatedly(
            w.autoscaler.keepalive, w.autoscaler.maybe_scale,
        )


class Autoscaler(bgThread):
    """Background thread to autoscale pool workers."""

    _last_scale_up: float = None

    def __init__(self, pool: PoolT, max_concurrency: int,
                 min_concurrency: int = 0,
                 worker: WorkerT = None,
                 keepalive: float = AUTOSCALE_KEEPALIVE,
                 mutex: threading.Lock = None) -> None:
        super(Autoscaler, self).__init__()
        self.pool = pool
        self.mutex = mutex or threading.Lock()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self.worker = worker

        assert self.keepalive, 'cannot scale down too fast.'

    def body(self) -> None:
        with self.mutex:
            self.maybe_scale()
        sleep(1.0)

    def _maybe_scale(self, req: RequestT = None) -> None:
        procs = self.processes
        cur = min(self.qty, self.max_concurrency)
        if cur > procs:
            self.scale_up(cur - procs)
            return True
        cur = max(self.qty, self.min_concurrency)
        if cur < procs:
            self.scale_down(procs - cur)
            return True

    def maybe_scale(self, req: RequestT = None) -> None:
        if self._maybe_scale(req):
            self.pool.maintain_pool()

    def update(self, max: int = None, min: int = None) -> Tuple[int, int]:
        with self.mutex:
            if max is not None:
                if max < self.processes:
                    self._shrink(self.processes - max)
                self.max_concurrency = max
            if min is not None:
                if min > self.processes:
                    self._grow(min - self.processes)
                self.min_concurrency = min
            return self.max_concurrency, self.min_concurrency

    def force_scale_up(self, n: int) -> None:
        with self.mutex:
            new = self.processes + n
            if new > self.max_concurrency:
                self.max_concurrency = new
            self._grow(n)

    def force_scale_down(self, n: int) -> None:
        with self.mutex:
            new = self.processes - n
            if new < self.min_concurrency:
                self.min_concurrency = max(new, 0)
            self._shrink(min(n, self.processes))

    def scale_up(self, n: int) -> None:
        self._last_scale_up = monotonic()
        self._grow(n)

    def scale_down(self, n: int) -> None:
        if self._last_scale_up and (
                monotonic() - self._last_scale_up > self.keepalive):
            self._shrink(n)

    def _grow(self, n: int) -> None:
        info('Scaling up %s processes.', n)
        self.pool.grow(n)
        self.worker.consumer._update_prefetch_count(n)

    def _shrink(self, n: int) -> None:
        info('Scaling down %s processes.', n)
        try:
            self.pool.shrink(n)
        except ValueError:
            debug("Autoscaler won't scale down: all processes busy.")
        except Exception as exc:
            error('Autoscaler: scale_down: %r', exc, exc_info=True)
        self.worker.consumer._update_prefetch_count(-n)

    def info(self) -> Mapping:
        return {
            'max': self.max_concurrency,
            'min': self.min_concurrency,
            'current': self.processes,
            'qty': self.qty,
        }

    @property
    def qty(self) -> int:
        return len(state.reserved_requests)

    @property
    def processes(self) -> int:
        return self.pool.num_processes

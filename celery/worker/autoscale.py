"""Pool Autoscaling.

This module implements the internal thread responsible
for growing and shrinking the pool according to the
current autoscale settings.

The autoscale thread is only enabled if
the :option:`celery worker --autoscale` option is used.
"""
import os
import threading
from time import monotonic, sleep

from kombu.asynchronous.semaphore import DummyLock

from celery import bootsteps
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

    def __init__(self, w, **kwargs):
        self.enabled = w.autoscale
        w.autoscaler = None

    def create(self, w):
        scaler = w.autoscaler = self.instantiate(
            w.autoscaler_cls,
            w.pool, w.max_concurrency, w.min_concurrency,
            worker=w, mutex=DummyLock() if w.use_eventloop else None,
        )
        return scaler if not w.use_eventloop else None

    def start(self, w):
        if not w.use_eventloop:
            on_task_received = getattr(w.autoscaler, 'on_task_received', None)
            if on_task_received is not None:
                w.consumer.on_task_message.add(on_task_received)
        super().start(w)

    def register_with_event_loop(self, w, hub):
        w.consumer.on_task_message.add(w.autoscaler.maybe_scale)
        hub.call_repeatedly(
            w.autoscaler.keepalive, w.autoscaler.maybe_scale,
        )

    def info(self, w):
        """Return `Autoscaler` info."""
        return {'autoscaler': w.autoscaler.info()}


class Autoscaler(bgThread):
    """Background thread to autoscale pool workers."""

    def __init__(self, pool, max_concurrency,
                 min_concurrency=0, worker=None,
                 keepalive=AUTOSCALE_KEEPALIVE, mutex=None):
        super().__init__()
        self.pool = pool
        self.mutex = mutex or threading.Lock()
        self.max_concurrency = max_concurrency
        self.min_concurrency = min_concurrency
        self.keepalive = keepalive
        self._last_scale_up = None
        self._last_task_received = None
        self.worker = worker

        assert self.keepalive, 'cannot scale down too fast.'

    def body(self):
        with self.mutex:
            self.maybe_scale()
        sleep(1.0)

    def _maybe_scale(self, req=None):
        procs = self.processes
        cur = min(self.qty, self.max_concurrency)
        if cur > procs:
            self.scale_up(cur - procs)
            return True
        cur = max(self.qty, self.min_concurrency)
        if cur < procs:
            self.scale_down(procs - cur)
            return True

    def maybe_scale(self, req=None):
        if req is not None:
            self.on_task_received(req)
        if self._maybe_scale(req):
            self.pool.maintain_pool()

    def on_task_received(self, req):
        self._last_task_received = monotonic()

    def update(self, max=None, min=None):
        with self.mutex:
            if max is not None:
                if max < self.processes:
                    self._shrink(self.processes - max)
                self._update_consumer_prefetch_count(max)
                self.max_concurrency = max
            if min is not None:
                if min > self.processes:
                    self._grow(min - self.processes)
                self.min_concurrency = min
            return self.max_concurrency, self.min_concurrency

    def scale_up(self, n):
        self._last_scale_up = monotonic()
        return self._grow(n)

    def scale_down(self, n):
        last_active = self._last_scale_up
        last_received = self._last_task_received
        if last_received is not None and (
                last_active is None or last_received > last_active):
            last_active = last_received
        if last_active is not None and (
                monotonic() - last_active > self.keepalive):
            return self._shrink(n)

    def _grow(self, n):
        info('Scaling up %s processes.', n)
        self.pool.grow(n)

    def _shrink(self, n):
        info('Scaling down %s processes.', n)
        try:
            self.pool.shrink(n)
        except ValueError:
            debug("Autoscaler won't scale down: all processes busy.")
        except Exception as exc:
            error('Autoscaler: scale_down: %r', exc, exc_info=True)

    def _update_consumer_prefetch_count(self, new_max):
        diff = new_max - self.max_concurrency
        if diff:
            self.worker.consumer._update_prefetch_count(
                diff
            )

    def info(self):
        return {
            'max': self.max_concurrency,
            'min': self.min_concurrency,
            'current': self.processes,
            'qty': self.qty,
        }

    @property
    def qty(self):
        return len(state.reserved_requests)

    @property
    def processes(self):
        return self.pool.num_processes

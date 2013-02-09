# -*- coding: utf-8 -*-
"""
    celery.worker.components
    ~~~~~~~~~~~~~~~~~~~~~~~~

    Default worker bootsteps.

"""
from __future__ import absolute_import

import atexit
import time

from functools import partial

from billiard.exceptions import WorkerLostError

from celery import bootsteps
from celery.five import items, string_t
from celery.utils.log import worker_logger as logger
from celery.utils.timer2 import Schedule

from . import hub
from .buckets import TaskBucket, FastQueue


class Hub(bootsteps.StartStopStep):

    def __init__(self, w, **kwargs):
        w.hub = None

    def include_if(self, w):
        return w.use_eventloop

    def create(self, w):
        w.timer = Schedule(max_interval=10)
        w.hub = hub.Hub(w.timer)
        return w.hub


class Queues(bootsteps.Step):
    """This bootstep initializes the internal queues
    used by the worker."""
    label = 'Queues (intra)'
    requires = (Hub, )

    def __init__(self, w, **kwargs):
        w.start_mediator = False

    def create(self, w):
        w.start_mediator = True
        if not w.pool_cls.rlimit_safe:
            w.disable_rate_limits = True
        if w.disable_rate_limits:
            w.ready_queue = FastQueue()
            if w.use_eventloop:
                w.start_mediator = False
                if w.pool_putlocks and w.pool_cls.uses_semaphore:
                    w.ready_queue.put = w.process_task_sem
                else:
                    w.ready_queue.put = w.process_task
            elif not w.pool_cls.requires_mediator:
                # just send task directly to pool, skip the mediator.
                w.ready_queue.put = w.process_task
                w.start_mediator = False
        else:
            w.ready_queue = TaskBucket(task_registry=w.app.tasks)


class Pool(bootsteps.StartStopStep):
    """Bootstep managing the worker pool.

    Describes how to initialize the worker pool, and starts and stops
    the pool during worker startup/shutdown.

    Adds attributes:

        * autoscale
        * pool
        * max_concurrency
        * min_concurrency

    """
    requires = (Queues, )

    def __init__(self, w, autoscale=None, autoreload=None,
                 no_execv=False, **kwargs):
        if isinstance(autoscale, string_t):
            max_c, _, min_c = autoscale.partition(',')
            autoscale = [int(max_c), min_c and int(min_c) or 0]
        w.autoscale = autoscale
        w.pool = None
        w.max_concurrency = None
        w.min_concurrency = w.concurrency
        w.no_execv = no_execv
        if w.autoscale:
            w.max_concurrency, w.min_concurrency = w.autoscale
        self.autoreload_enabled = autoreload

    def close(self, w):
        if w.pool:
            w.pool.close()

    def terminate(self, w):
        if w.pool:
            w.pool.terminate()

    def on_poll_init(self, pool, w, hub):
        apply_after = hub.timer.apply_after
        apply_at = hub.timer.apply_at
        on_soft_timeout = pool.on_soft_timeout
        on_hard_timeout = pool.on_hard_timeout
        maintain_pool = pool.maintain_pool
        add_reader = hub.add_reader
        remove = hub.remove
        now = time.time

        # did_start_ok will verify that pool processes were able to start,
        # but this will only work the first time we start, as
        # maxtasksperchild will mess up metrics.
        if not w.consumer.restart_count and not pool.did_start_ok():
            raise WorkerLostError('Could not start worker processes')

        # need to handle pool results before every task
        # since multiple tasks can be received in a single poll()
        hub.on_task.append(pool.maybe_handle_result)

        hub.update_readers(pool.readers)
        for handler, interval in items(pool.timers):
            hub.timer.apply_interval(interval * 1000.0, handler)

        def on_timeout_set(R, soft, hard):

            def _on_soft_timeout():
                if hard:
                    R._tref = apply_at(now() + (hard - soft),
                                       on_hard_timeout, (R, ))
                on_soft_timeout(R)
            if soft:
                R._tref = apply_after(soft * 1000.0, _on_soft_timeout)
            elif hard:
                R._tref = apply_after(hard * 1000.0,
                                      on_hard_timeout, (R, ))

        def on_timeout_cancel(result):
            try:
                result._tref.cancel()
                delattr(result, '_tref')
            except AttributeError:
                pass

        pool.init_callbacks(
            on_process_up=lambda w: add_reader(w.sentinel, maintain_pool),
            on_process_down=lambda w: remove(w.sentinel),
            on_timeout_set=on_timeout_set,
            on_timeout_cancel=on_timeout_cancel,
        )

    def create(self, w, semaphore=None, max_restarts=None):
        threaded = not w.use_eventloop
        procs = w.min_concurrency
        forking_enable = not threaded or (w.no_execv or not w.force_execv)
        if not threaded:
            semaphore = w.semaphore = hub.BoundedSemaphore(procs)
            w._quick_acquire = w.semaphore.acquire
            w._quick_release = w.semaphore.release
            max_restarts = 100
        allow_restart = self.autoreload_enabled or w.pool_restarts
        pool = w.pool = self.instantiate(
            w.pool_cls, w.min_concurrency,
            initargs=(w.app, w.hostname),
            maxtasksperchild=w.max_tasks_per_child,
            timeout=w.task_time_limit,
            soft_timeout=w.task_soft_time_limit,
            putlocks=w.pool_putlocks and threaded,
            lost_worker_timeout=w.worker_lost_wait,
            threads=threaded,
            max_restarts=max_restarts,
            allow_restart=allow_restart,
            forking_enable=forking_enable,
            semaphore=semaphore,
        )
        if w.hub:
            w.hub.on_init.append(partial(self.on_poll_init, pool, w))
        return pool

    def info(self, w):
        return {'pool': w.pool.info}


class Beat(bootsteps.StartStopStep):
    """Step used to embed a beat process.

    This will only be enabled if the ``beat``
    argument is set.

    """
    label = 'Beat'
    conditional = True

    def __init__(self, w, beat=False, **kwargs):
        self.enabled = w.beat = beat
        w.beat = None

    def create(self, w):
        from celery.beat import EmbeddedService
        b = w.beat = EmbeddedService(app=w.app,
                                     schedule_filename=w.schedule_filename,
                                     scheduler_cls=w.scheduler_cls)
        return b


class Timer(bootsteps.Step):
    """This step initializes the internal timer used by the worker."""
    requires = (Pool, )

    def include_if(self, w):
        return not w.use_eventloop

    def create(self, w):
        if not w.timer_cls:
            # Default Timer is set by the pool, as e.g. eventlet
            # needs a custom implementation.
            w.timer_cls = w.pool.Timer
        w.timer = self.instantiate(w.pool.Timer,
                                   max_interval=w.timer_precision,
                                   on_timer_error=self.on_timer_error,
                                   on_timer_tick=self.on_timer_tick)

    def on_timer_error(self, exc):
        logger.error('Timer error: %r', exc, exc_info=True)

    def on_timer_tick(self, delay):
        logger.debug('Timer wake-up! Next eta %s secs.', delay)


class StateDB(bootsteps.Step):
    """This bootstep sets up the workers state db if enabled."""

    def __init__(self, w, **kwargs):
        self.enabled = w.state_db
        w._persistence = None

    def create(self, w):
        w._persistence = w.state.Persistent(w.state_db, w.app.clock)
        atexit.register(w._persistence.save)


class Consumer(bootsteps.StartStopStep):
    last = True

    def create(self, w):
        prefetch_count = w.concurrency * w.prefetch_multiplier
        c = w.consumer = self.instantiate(
            w.consumer_cls, w.ready_queue,
            hostname=w.hostname,
            send_events=w.send_events,
            init_callback=w.ready_callback,
            initial_prefetch_count=prefetch_count,
            pool=w.pool,
            timer=w.timer,
            app=w.app,
            controller=w,
            hub=w.hub,
            worker_options=w.options,
        )
        return c

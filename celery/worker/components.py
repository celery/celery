# -*- coding: utf-8 -*-
"""
    celery.worker.components
    ~~~~~~~~~~~~~~~~~~~~~~~~

    Default worker bootsteps.

"""
from __future__ import absolute_import

import atexit

from functools import partial

from celery import bootsteps
from celery.exceptions import ImproperlyConfigured
from celery.five import string_t
from celery.utils.log import worker_logger as logger
from celery.utils.timer2 import Schedule

from . import hub

ERR_B_GREEN = """\
-B option doesn't work with eventlet/gevent pools: \
use standalone beat instead.\
"""


class Object(object):  # XXX
    pass


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

    def create(self, w):
        w.process_task = w._process_task
        if w.use_eventloop:
            if w.pool_putlocks and w.pool_cls.uses_semaphore:
                w.process_task = w._process_task_sem


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
            w.hub.on_init.append(partial(pool.on_poll_init, w))
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
        if w.pool_cls.__module__.endswith(('gevent', 'eventlet')):
            raise ImproperlyConfigured(ERR_B_GREEN)
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
            w.consumer_cls, w.process_task,
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
            disable_rate_limits=w.disable_rate_limits,
        )
        return c

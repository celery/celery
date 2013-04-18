# -*- coding: utf-8 -*-
"""
    celery.worker
    ~~~~~~~~~~~~~

    :class:`WorkController` can be used to instantiate in-process workers.

    The worker consists of several components, all managed by boot-steps
    (mod:`celery.worker.bootsteps`).

"""
from __future__ import absolute_import

import atexit
import logging
import socket
import sys
import time
import traceback

from functools import partial

from billiard.exceptions import WorkerLostError
from billiard.util import Finalize
from kombu.syn import detect_environment

from celery import concurrency as _concurrency
from celery import platforms
from celery.app import app_or_default
from celery.app.abstract import configurated, from_config
from celery.exceptions import SystemTerminate, TaskRevokedError
from celery.utils.functional import noop
from celery.utils.imports import qualname, reload_from_cwd
from celery.utils.log import get_logger
from celery.utils.threads import Event
from celery.utils.timer2 import Schedule

from . import bootsteps
from . import state
from .buckets import TaskBucket, AsyncTaskBucket, FastQueue
from .hub import Hub, BoundedSemaphore

#: Worker states
RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

#: Default socket timeout at shutdown.
SHUTDOWN_SOCKET_TIMEOUT = 5.0

logger = get_logger(__name__)


class Namespace(bootsteps.Namespace):
    """This is the boot-step namespace of the :class:`WorkController`.

    It loads modules from :setting:`CELERYD_BOOT_STEPS`, and its
    own set of built-in boot-step modules.

    """
    name = 'worker'
    builtin_boot_steps = ('celery.worker.autoscale',
                          'celery.worker.autoreload',
                          'celery.worker.consumer',
                          'celery.worker.mediator')

    def modules(self):
        return self.builtin_boot_steps + self.app.conf.CELERYD_BOOT_STEPS


class Pool(bootsteps.StartStopComponent):
    """The pool component.

    Describes how to initialize the worker pool, and starts and stops
    the pool during worker startup/shutdown.

    Adds attributes:

        * autoscale
        * pool
        * max_concurrency
        * min_concurrency

    """
    name = 'worker.pool'
    requires = ('queues', 'beat', )

    def __init__(self, w,
                 autoscale=None, autoreload=False, no_execv=False, **kwargs):
        w.autoscale = autoscale
        w.pool = None
        w.max_concurrency = None
        w.min_concurrency = w.concurrency
        w.no_execv = no_execv
        if w.autoscale:
            w.max_concurrency, w.min_concurrency = w.autoscale
        self.autoreload_enabled = autoreload

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
        for handler, interval in pool.timers.iteritems():
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
        forking_enable = w.no_execv or not w.force_execv
        if not threaded:
            semaphore = w.semaphore = BoundedSemaphore(procs)
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
            callbacks_propagate=(
                w._conninfo.connection_errors + w._conninfo.channel_errors
            ),
        )
        if w.hub:
            w.hub.on_init.append(partial(self.on_poll_init, pool, w))
        return pool


class Beat(bootsteps.StartStopComponent):
    """Component used to embed a celerybeat process.

    This will only be enabled if the ``beat``
    argument is set.

    """
    name = 'worker.beat'

    def __init__(self, w, beat=False, **kwargs):
        self.enabled = w.beat = beat
        w.beat = None

    def create(self, w):
        from celery.beat import EmbeddedService
        b = w.beat = EmbeddedService(app=w.app,
                                     schedule_filename=w.schedule_filename,
                                     scheduler_cls=w.scheduler_cls)
        return b


class Queues(bootsteps.Component):
    """This component initializes the internal queues
    used by the worker."""
    name = 'worker.queues'
    requires = ('ev', )

    def create(self, w):
        BucketType = TaskBucket
        w.start_mediator = not w.disable_rate_limits
        if not w.pool_cls.rlimit_safe:
            w.start_mediator = False
            BucketType = AsyncTaskBucket
        process_task = w.process_task
        if w.use_eventloop:
            w.start_mediator = False
            BucketType = AsyncTaskBucket
            if w.pool_putlocks and w.pool_cls.uses_semaphore:
                process_task = w.process_task_sem
        if w.disable_rate_limits:
            w.ready_queue = FastQueue()
            w.ready_queue.put = process_task
        else:
            w.ready_queue = BucketType(
                task_registry=w.app.tasks, callback=process_task, worker=w,
            )


class EvLoop(bootsteps.StartStopComponent):
    name = 'worker.ev'

    def __init__(self, w, **kwargs):
        w.hub = None

    def include_if(self, w):
        return w.use_eventloop

    def create(self, w):
        w.timer = Schedule(max_interval=10)
        hub = w.hub = Hub(w.timer)
        return hub


class Timers(bootsteps.Component):
    """This component initializes the internal timers used by the worker."""
    name = 'worker.timers'
    requires = ('pool', )

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


class StateDB(bootsteps.Component):
    """This component sets up the workers state db if enabled."""
    name = 'worker.state-db'

    def __init__(self, w, **kwargs):
        self.enabled = w.state_db
        w._persistence = None

    def create(self, w):
        w._persistence = state.Persistent(w.state_db)
        atexit.register(w._persistence.save)


class WorkController(configurated):
    """Unmanaged worker instance."""
    RUN = RUN
    CLOSE = CLOSE
    TERMINATE = TERMINATE

    app = None
    concurrency = from_config()
    loglevel = logging.ERROR
    logfile = from_config('log_file')
    send_events = from_config()
    pool_cls = from_config('pool')
    consumer_cls = from_config('consumer')
    mediator_cls = from_config('mediator')
    timer_cls = from_config('timer')
    timer_precision = from_config('timer_precision')
    autoscaler_cls = from_config('autoscaler')
    autoreloader_cls = from_config('autoreloader')
    schedule_filename = from_config()
    scheduler_cls = from_config('celerybeat_scheduler')
    task_time_limit = from_config()
    task_soft_time_limit = from_config()
    max_tasks_per_child = from_config()
    pool_putlocks = from_config()
    pool_restarts = from_config()
    force_execv = from_config()
    prefetch_multiplier = from_config()
    state_db = from_config()
    disable_rate_limits = from_config()
    worker_lost_wait = from_config()

    _state = None
    _running = 0

    def __init__(self, loglevel=None, hostname=None, ready_callback=noop,
                 queues=None, app=None, pidfile=None, use_eventloop=None,
                 **kwargs):
        self.app = app_or_default(app or self.app)

        self._shutdown_complete = Event()
        self.setup_defaults(kwargs, namespace='celeryd')
        self.app.select_queues(queues)  # select queues subset.

        # Options
        self.loglevel = loglevel or self.loglevel
        self.hostname = hostname or socket.gethostname()
        self.ready_callback = ready_callback
        self._finalize = Finalize(self, self.stop, exitpriority=1)
        self.pidfile = pidfile
        self.pidlock = None
        # this connection is not established, only used for params
        self._conninfo = self.app.connection()
        self.use_eventloop = (
            self.should_use_eventloop() if use_eventloop is None
            else use_eventloop
        )

        # Update celery_include to have all known task modules, so that we
        # ensure all task modules are imported in case an execv happens.
        task_modules = set(task.__class__.__module__
                           for task in self.app.tasks.itervalues())
        self.app.conf.CELERY_INCLUDE = tuple(
            set(self.app.conf.CELERY_INCLUDE) | task_modules,
        )

        # Initialize boot steps
        self.pool_cls = _concurrency.get_implementation(self.pool_cls)
        self.components = []
        self.namespace = Namespace(app=self.app).apply(self, **kwargs)

    def start(self):
        """Starts the workers main loop."""
        self._state = self.RUN
        if self.pidfile:
            self.pidlock = platforms.create_pidlock(self.pidfile)
        try:
            for i, component in enumerate(self.components):
                logger.debug('Starting %s...', qualname(component))
                self._running = i + 1
                if component:
                    component.start()
                logger.debug('%s OK!', qualname(component))
        except SystemTerminate:
            self.terminate()
        except Exception, exc:
            logger.error('Unrecoverable error: %r', exc,
                         exc_info=True)
            self.stop()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

        # Will only get here if running green,
        # makes sure all greenthreads have exited.
        self._shutdown_complete.wait()

    def process_task_sem(self, req):
        return self._quick_acquire(self.process_task, req)

    def process_task(self, req):
        """Process task by sending it to the pool of workers."""
        try:
            req.execute_using_pool(self.pool)
        except TaskRevokedError:
            try:
                self._quick_release()   # Issue 877
            except AttributeError:
                pass
        except Exception, exc:
            logger.critical('Internal error: %r\n%s',
                            exc, traceback.format_exc(), exc_info=True)
        except SystemTerminate:
            self.terminate()
            raise
        except BaseException, exc:
            self.stop()
            raise exc

    def signal_consumer_close(self):
        try:
            self.consumer.close()
        except AttributeError:
            pass

    def should_use_eventloop(self):
        return (detect_environment() == 'default' and
                self._conninfo.is_evented and not self.app.IS_WINDOWS)

    def stop(self, in_sighandler=False):
        """Graceful shutdown of the worker server."""
        self.signal_consumer_close()
        if not in_sighandler or self.pool.signal_safe:
            self._shutdown(warm=True)

    def terminate(self, in_sighandler=False):
        """Not so graceful shutdown of the worker server."""
        self.signal_consumer_close()
        if not in_sighandler or self.pool.signal_safe:
            self._shutdown(warm=False)

    def _shutdown(self, warm=True):
        what = 'Stopping' if warm else 'Terminating'
        socket_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(SHUTDOWN_SOCKET_TIMEOUT)  # Issue 975

        if self._state in (self.CLOSE, self.TERMINATE):
            return

        self.app.loader.shutdown_worker()

        if self.pool:
            self.pool.close()

        if self._state != self.RUN or self._running != len(self.components):
            # Not fully started, can safely exit.
            self._state = self.TERMINATE
            self._shutdown_complete.set()
            return
        self._state = self.CLOSE

        for component in reversed(self.components):
            logger.debug('%s %s...', what, qualname(component))
            if component:
                stop = component.stop
                if not warm:
                    stop = getattr(component, 'terminate', None) or stop
                stop()

        self.timer.stop()
        self.consumer.close_connection()

        if self.pidlock:
            self.pidlock.release()
        self._state = self.TERMINATE
        socket.setdefaulttimeout(socket_timeout)
        self._shutdown_complete.set()

    def reload(self, modules=None, reload=False, reloader=None):
        modules = self.app.loader.task_modules if modules is None else modules
        imp = self.app.loader.import_from_cwd

        for module in set(modules or ()):
            if module not in sys.modules:
                logger.debug('importing module %s', module)
                imp(module)
            elif reload:
                logger.debug('reloading module %s', module)
                reload_from_cwd(sys.modules[module], reloader)
        self.pool.restart()

    @property
    def state(self):
        return state

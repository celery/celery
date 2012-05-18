# -*- coding: utf-8 -*-
"""
    celery.worker
    ~~~~~~~~~~~~~

    :class:`WorkController` can be used to instantiate in-process workers.

    The worker consists of several components, all managed by boot-steps
    (mod:`celery.abstract`).

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import atexit
import logging
import socket
import sys
import traceback

from billiard import forking_enable
from kombu.syn import detect_environment
from kombu.utils.finalize import Finalize

from celery import concurrency as _concurrency
from celery import platforms
from celery.app import app_or_default, set_default_app
from celery.app.abstract import configurated, from_config
from celery.exceptions import SystemTerminate
from celery.utils.functional import noop
from celery.utils.imports import qualname, reload_from_cwd
from celery.utils.log import get_logger
from celery.utils.threads import Event
from celery.utils.timer2 import Schedule

from . import abstract
from . import state
from .buckets import TaskBucket, FastQueue

RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

logger = get_logger(__name__)


class Namespace(abstract.Namespace):
    """This is the boot-step namespace of the :class:`WorkController`.

    It loads modules from :setting:`CELERYD_BOOT_STEPS`, and its
    own set of built-in boot-step modules.

    """
    name = "worker"
    builtin_boot_steps = ("celery.worker.autoscale",
                          "celery.worker.autoreload",
                          "celery.worker.consumer",
                          "celery.worker.mediator")

    def modules(self):
        return (self.builtin_boot_steps
              + self.app.conf.CELERYD_BOOT_STEPS)


class Pool(abstract.StartStopComponent):
    """The pool component.

    Describes how to initialize the worker pool, and starts and stops
    the pool during worker startup/shutdown.

    Adds attributes:

        * autoscale
        * pool
        * max_concurrency
        * min_concurrency

    """
    name = "worker.pool"
    requires = ("queues", )

    def __init__(self, w, autoscale=None, no_execv=False, **kwargs):
        w.autoscale = autoscale
        w.pool = None
        w.max_concurrency = None
        w.min_concurrency = w.concurrency
        w.no_execv = no_execv
        if w.autoscale:
            w.max_concurrency, w.min_concurrency = w.autoscale

    def create(self, w):
        threaded = not w.use_eventloop
        forking_enable(w.no_execv or not w.force_execv)
        pool = w.pool = self.instantiate(w.pool_cls, w.min_concurrency,
                            initargs=(w.app, w.hostname),
                            maxtasksperchild=w.max_tasks_per_child,
                            timeout=w.task_time_limit,
                            soft_timeout=w.task_soft_time_limit,
                            putlocks=w.pool_putlocks,
                            lost_worker_timeout=w.worker_lost_wait,
                            with_task_thread=threaded,
                            with_result_thread=threaded,
                            with_supervisor_thread=threaded)
        return pool


class Beat(abstract.StartStopComponent):
    """Component used to embed a celerybeat process.

    This will only be enabled if the ``beat``
    argument is set.

    """
    name = "worker.beat"

    def __init__(self, w, beat=False, **kwargs):
        self.enabled = w.beat = beat
        w.beat = None

    def create(self, w):
        from celery.beat import EmbeddedService
        b = w.beat = EmbeddedService(app=w.app,
                                     schedule_filename=w.schedule_filename,
                                     scheduler_cls=w.scheduler_cls)
        return b


class Queues(abstract.Component):
    """This component initializes the internal queues
    used by the worker."""
    name = "worker.queues"

    def create(self, w):
        if not w.pool_cls.rlimit_safe:
            w.disable_rate_limits = True
        if w.disable_rate_limits:
            w.ready_queue = FastQueue()
            if not w.pool_cls.requires_mediator:
                # just send task directly to pool, skip the mediator.
                w.ready_queue.put = w.process_task
        else:
            w.ready_queue = TaskBucket(task_registry=w.app.tasks)


class Timers(abstract.Component):
    """This component initializes the internal timers used by the worker."""
    name = "worker.timers"
    requires = ("pool", )

    def create(self, w):
        options = {"on_error": self.on_timer_error,
                   "on_tick": self.on_timer_tick}

        if w.use_eventloop:
            # the timers are fired by the hub, so don't use the Timer thread.
            w.timer = Schedule(max_interval=10, **options)
        else:
            if not w.timer_cls:
                # Default Timer is set by the pool, as e.g. eventlet
                # needs a custom implementation.
                w.timer_cls = w.pool.Timer
            w.timer = self.instantiate(w.pool.Timer,
                                       max_interval=w.timer_precision,
                                       **options)

    def on_timer_error(self, exc):
        logger.error("Timer error: %r", exc, exc_info=True)

    def on_timer_tick(self, delay):
        logger.debug("Timer wake-up! Next eta %s secs.", delay)


class StateDB(abstract.Component):
    """This component sets up the workers state db if enabled."""
    name = "worker.state-db"

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
    logfile = from_config("log_file")
    send_events = from_config()
    pool_cls = from_config("pool")
    consumer_cls = from_config("consumer")
    mediator_cls = from_config("mediator")
    timer_cls = from_config("timer")
    timer_precision = from_config("timer_precision")
    autoscaler_cls = from_config("autoscaler")
    autoreloader_cls = from_config("autoreloader")
    schedule_filename = from_config()
    scheduler_cls = from_config("celerybeat_scheduler")
    task_time_limit = from_config()
    task_soft_time_limit = from_config()
    max_tasks_per_child = from_config()
    pool_putlocks = from_config()
    force_execv = from_config()
    prefetch_multiplier = from_config()
    state_db = from_config()
    disable_rate_limits = from_config()
    worker_lost_wait = from_config()

    _state = None
    _running = 0

    def __init__(self, loglevel=None, hostname=None, ready_callback=noop,
            queues=None, app=None, pidfile=None, **kwargs):
        self.app = app_or_default(app or self.app)

        # all new threads start without a current app, so if an app is not
        # passed on to the thread it will fall back to the "default app",
        # which then could be the wrong app.  So for the worker
        # we set this to always return our app.  This is a hack,
        # and means that only a single app can be used for workers
        # running in the same process.
        set_default_app(self.app)

        self._shutdown_complete = Event()
        self.setup_defaults(kwargs, namespace="celeryd")
        self.app.select_queues(queues)  # select queues subset.

        # Options
        self.loglevel = loglevel or self.loglevel
        self.hostname = hostname or socket.gethostname()
        self.ready_callback = ready_callback
        self._finalize = Finalize(self, self.stop, exitpriority=1)
        self.pidfile = pidfile
        self.pidlock = None
        self.use_eventloop = (detect_environment() == "default" and
                              self.app.broker_connection().is_evented)

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
                logger.debug("Starting %s...", qualname(component))
                self._running = i + 1
                component.start()
                logger.debug("%s OK!", qualname(component))
        except SystemTerminate:
            self.terminate()
        except Exception, exc:
            logger.error("Unrecoverable error: %r", exc,
                         exc_info=True)
            self.stop()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

        # Will only get here if running green,
        # makes sure all greenthreads have exited.
        self._shutdown_complete.wait()

    def process_task(self, req):
        """Process task by sending it to the pool of workers."""
        try:
            req.task.execute(req, self.pool, self.loglevel, self.logfile)
        except Exception, exc:
            logger.critical("Internal error: %r\n%s",
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
        what = "Stopping" if warm else "Terminating"

        if self._state in (self.CLOSE, self.TERMINATE):
            return

        if self.pool:
            self.pool.close()

        if self._state != self.RUN or self._running != len(self.components):
            # Not fully started, can safely exit.
            self._state = self.TERMINATE
            self._shutdown_complete.set()
            return
        self._state = self.CLOSE

        for component in reversed(self.components):
            logger.debug("%s %s...", what, qualname(component))
            stop = component.stop
            if not warm:
                stop = getattr(component, "terminate", None) or stop
            stop()

        self.timer.stop()
        self.consumer.close_connection()

        if self.pidlock:
            self.pidlock.release()
        self._state = self.TERMINATE
        self._shutdown_complete.set()

    def reload(self, modules=None, reload=False, reloader=None):
        modules = self.app.loader.task_modules if modules is None else modules
        imp = self.app.loader.import_from_cwd

        for module in set(modules or ()):
            if module not in sys.modules:
                logger.debug("importing module %s", module)
                imp(module)
            elif reload:
                logger.debug("reloading module %s", module)
                reload_from_cwd(sys.modules[module], reloader)
        self.pool.restart()

    @property
    def state(self):
        return state

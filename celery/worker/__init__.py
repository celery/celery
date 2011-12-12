# -*- coding: utf-8 -*-
"""
    celery.worker
    ~~~~~~~~~~~~~

    The worker.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import atexit
import logging
import socket
import sys
import threading
import traceback

from kombu.utils.finalize import Finalize

from .. import beat
from .. import concurrency as _concurrency
from .. import registry, signals
from ..app import app_or_default
from ..app.abstract import configurated, from_config
from ..exceptions import SystemTerminate
from ..log import SilenceRepeated
from ..utils import noop, instantiate

from . import state
from .buckets import TaskBucket, FastQueue

RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3


class WorkController(configurated):
    """Unmanaged worker instance."""
    RUN = RUN
    CLOSE = CLOSE
    TERMINATE = TERMINATE

    concurrency = from_config()
    loglevel = logging.ERROR
    logfile = from_config("log_file")
    send_events = from_config()
    pool_cls = from_config("pool")
    consumer_cls = from_config("consumer")
    mediator_cls = from_config("mediator")
    eta_scheduler_cls = from_config("eta_scheduler")
    eta_scheduler_precision = from_config()
    autoscaler_cls = from_config("autoscaler")
    schedule_filename = from_config()
    scheduler_cls = from_config("celerybeat_scheduler")
    task_time_limit = from_config()
    task_soft_time_limit = from_config()
    max_tasks_per_child = from_config()
    pool_putlocks = from_config()
    prefetch_multiplier = from_config()
    state_db = from_config()
    disable_rate_limits = from_config()

    _state = None
    _running = 0
    _persistence = None

    def __init__(self, loglevel=None, hostname=None, logger=None,
            ready_callback=noop, embed_clockservice=False, autoscale=None,
            queues=None, app=None, **kwargs):
        self.app = app_or_default(app)
        conf = self.app.conf
        self._shutdown_complete = threading.Event()
        self.setup_defaults(kwargs, namespace="celeryd")
        self.app.select_queues(queues)  # select queues subset.

        # Options
        self.pool_cls = _concurrency.get_implementation(self.pool_cls)
        self.autoscale = autoscale
        self.loglevel = loglevel or self.loglevel
        self.logger = self.app.log.get_default_logger()
        self.hostname = hostname or socket.gethostname()
        self.embed_clockservice = embed_clockservice
        self.ready_callback = ready_callback
        self.timer_debug = SilenceRepeated(self.logger.debug,
                                           max_iterations=10)
        self._finalize = Finalize(self, self.stop, exitpriority=1)
        self._finalize_db = None

        if self.state_db:
            self._persistence = state.Persistent(self.state_db)
            atexit.register(self._persistence.save)

        # Queues
        if not self.pool_cls.rlimit_safe:
            self.disable_rate_limits = True
        if self.disable_rate_limits:
            self.ready_queue = FastQueue()
            self.ready_queue.put = self.process_task
        else:
            self.ready_queue = TaskBucket(task_registry=registry.tasks)

        self.logger.debug("Instantiating thread components...")

        # Threads + Pool + Consumer
        self.autoscaler = None
        max_concurrency = None
        min_concurrency = self.concurrency
        if autoscale:
            max_concurrency, min_concurrency = autoscale

        self.pool = instantiate(self.pool_cls, min_concurrency,
                                logger=self.logger,
                                initargs=(self.app, self.hostname),
                                maxtasksperchild=self.max_tasks_per_child,
                                timeout=self.task_time_limit,
                                soft_timeout=self.task_soft_time_limit,
                                putlocks=self.pool_putlocks)
        self.priority_timer = instantiate(self.pool.Timer)

        if not self.eta_scheduler_cls:
            # Default Timer is set by the pool, as e.g. eventlet
            # needs a custom implementation.
            self.eta_scheduler_cls = self.pool.Timer

        self.autoscaler = None
        if autoscale:
            self.autoscaler = instantiate(self.autoscaler_cls, self.pool,
                                          max_concurrency=max_concurrency,
                                          min_concurrency=min_concurrency,
                                          logger=self.logger)

        self.mediator = None
        if not self.disable_rate_limits:
            self.mediator = instantiate(self.mediator_cls, self.ready_queue,
                                        app=self.app,
                                        callback=self.process_task,
                                        logger=self.logger)

        self.scheduler = instantiate(self.eta_scheduler_cls,
                                precision=self.eta_scheduler_precision,
                                on_error=self.on_timer_error,
                                on_tick=self.on_timer_tick)

        self.beat = None
        if self.embed_clockservice:
            self.beat = beat.EmbeddedService(app=self.app,
                                logger=self.logger,
                                schedule_filename=self.schedule_filename,
                                scheduler_cls=self.scheduler_cls)

        prefetch_count = self.concurrency * self.prefetch_multiplier
        self.consumer = instantiate(self.consumer_cls,
                                    self.ready_queue,
                                    self.scheduler,
                                    logger=self.logger,
                                    hostname=self.hostname,
                                    send_events=self.send_events,
                                    init_callback=self.ready_callback,
                                    initial_prefetch_count=prefetch_count,
                                    pool=self.pool,
                                    priority_timer=self.priority_timer,
                                    app=self.app,
                                    controller=self)

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.components = filter(None, (self.pool,
                                        self.mediator,
                                        self.beat,
                                        self.autoscaler,
                                        self.consumer))

    def start(self):
        """Starts the workers main loop."""
        self._state = self.RUN

        try:
            for i, component in enumerate(self.components):
                self.logger.debug("Starting thread %s...",
                                  component.__class__.__name__)
                self._running = i + 1
                component.start()
        except SystemTerminate:
            self.terminate()
        except Exception, exc:
            self.logger.error("Unrecoverable error: %r" % (exc, ),
                              exc_info=sys.exc_info())
            self.stop()
        except (KeyboardInterrupt, SystemExit):
            self.stop()

        # Will only get here if running green,
        # makes sure all greenthreads have exited.
        self._shutdown_complete.wait()

    def process_task(self, request):
        """Process task by sending it to the pool of workers."""
        try:
            request.task.execute(request, self.pool,
                                 self.loglevel, self.logfile)
        except Exception, exc:
            self.logger.critical("Internal error %s: %s\n%s",
                                 exc.__class__, exc, traceback.format_exc(),
                                 exc_info=True)
        except SystemTerminate:
            self.terminate()
            raise
        except BaseException, exc:
            self.stop()
            raise exc

    def stop(self, in_sighandler=False):
        """Graceful shutdown of the worker server."""
        if not in_sighandler or self.pool.signal_safe:
            self._shutdown(warm=True)

    def terminate(self, in_sighandler=False):
        """Not so graceful shutdown of the worker server."""
        if not in_sighandler or self.pool.signal_safe:
            self._shutdown(warm=False)

    def _shutdown(self, warm=True):
        what = (warm and "stopping" or "terminating").capitalize()

        if self._state in (self.CLOSE, self.TERMINATE):
            return

        if self._state != self.RUN or self._running != len(self.components):
            # Not fully started, can safely exit.
            self._state = self.TERMINATE
            self._shutdown_complete.set()
            return

        self._state = self.CLOSE
        signals.worker_shutdown.send(sender=self)

        for component in reversed(self.components):
            self.logger.debug("%s thread %s...", what,
                              component.__class__.__name__)
            stop = component.stop
            if not warm:
                stop = getattr(component, "terminate", None) or stop
            stop()

        self.priority_timer.stop()
        self.consumer.close_connection()

        self._state = self.TERMINATE
        self._shutdown_complete.set()

    def on_timer_error(self, exc_info):
        _, exc, _ = exc_info
        self.logger.error("Timer error: %r", exc, exc_info=exc_info)

    def on_timer_tick(self, delay):
        self.timer_debug("Scheduler wake-up! Next eta %s secs." % delay)

    @property
    def state(self):
        return state

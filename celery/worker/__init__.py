"""

The Multiprocessing Worker Server

"""
import socket
import logging
import traceback
from multiprocessing.util import Finalize

from celery import beat
from celery import registry
from celery import platform
from celery import signals
from celery.app import app_or_default
from celery.log import SilenceRepeated
from celery.utils import noop, instantiate

from celery.worker import state
from celery.worker.buckets import TaskBucket, FastQueue

RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

WORKER_SIGRESET = frozenset(["SIGTERM",
                             "SIGHUP",
                             "SIGTTIN",
                             "SIGTTOU"])
WORKER_SIGIGNORE = frozenset(["SIGINT"])


def process_initializer():
    """Initializes the process so it can be used to process tasks.

    Used for multiprocessing environments.

    """
    map(platform.reset_signal, WORKER_SIGRESET)
    map(platform.ignore_signal, WORKER_SIGIGNORE)
    platform.set_mp_process_title("celeryd")

    # This is for windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    from celery.loaders import current_loader
    current_loader().init_worker()

    signals.worker_process_init.send(sender=None)


class WorkController(object):
    loglevel = logging.ERROR
    _state = None
    _running = 0

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            send_events=None, hostname=None, ready_callback=noop,
            embed_clockservice=False, pool_cls=None, listener_cls=None,
            mediator_cls=None, eta_scheduler_cls=None,
            schedule_filename=None, task_time_limit=None,
            task_soft_time_limit=None, max_tasks_per_child=None,
            pool_putlocks=None, db=None, prefetch_multiplier=None,
            eta_scheduler_precision=None, queues=None,
            disable_rate_limits=None, app=None):

        self.app = app_or_default(app)
        conf = self.app.conf

        # Options
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or conf.CELERYD_CONCURRENCY
        self.logfile = logfile or conf.CELERYD_LOG_FILE
        self.logger = self.app.log.get_default_logger()
        if send_events is None:
            send_events = conf.CELERY_SEND_EVENTS
        self.send_events = send_events
        self.pool_cls = pool_cls or conf.CELERYD_POOL
        self.listener_cls = listener_cls or conf.CELERYD_LISTENER
        self.mediator_cls = mediator_cls or conf.CELERYD_MEDIATOR
        self.eta_scheduler_cls = eta_scheduler_cls or \
                                    conf.CELERYD_ETA_SCHEDULER
        self.schedule_filename = schedule_filename or \
                                    conf.CELERYBEAT_SCHEDULE_FILENAME
        self.hostname = hostname or socket.gethostname()
        self.embed_clockservice = embed_clockservice
        self.ready_callback = ready_callback
        self.task_time_limit = task_time_limit or \
                                conf.CELERYD_TASK_TIME_LIMIT
        self.task_soft_time_limit = task_soft_time_limit or \
                                conf.CELERYD_TASK_SOFT_TIME_LIMIT
        self.max_tasks_per_child = max_tasks_per_child or \
                                conf.CELERYD_MAX_TASKS_PER_CHILD
        self.pool_putlocks = pool_putlocks or \
                                conf.CELERYD_POOL_PUTLOCKS
        self.eta_scheduler_precision = eta_scheduler_precision or \
                                conf.CELERYD_ETA_SCHEDULER_PRECISION
        self.prefetch_multiplier = prefetch_multiplier or \
                                conf.CELERYD_PREFETCH_MULTIPLIER
        self.timer_debug = SilenceRepeated(self.logger.debug,
                                           max_iterations=10)
        self.db = db or conf.CELERYD_STATE_DB
        self.disable_rate_limits = disable_rate_limits or \
                                conf.CELERY_DISABLE_RATE_LIMITS
        self.queues = queues

        self._finalize = Finalize(self, self.stop, exitpriority=1)

        if self.db:
            persistence = state.Persistent(self.db)
            Finalize(persistence, persistence.save, exitpriority=5)

        # Queues
        if disable_rate_limits:
            self.ready_queue = FastQueue()
        else:
            self.ready_queue = TaskBucket(task_registry=registry.tasks)

        self.logger.debug("Instantiating thread components...")

        # Threads + Pool + Consumer
        self.pool = instantiate(self.pool_cls, self.concurrency,
                                logger=self.logger,
                                initializer=process_initializer,
                                maxtasksperchild=self.max_tasks_per_child,
                                timeout=self.task_time_limit,
                                soft_timeout=self.task_soft_time_limit,
                                putlocks=self.pool_putlocks)
        self.mediator = instantiate(self.mediator_cls, self.ready_queue,
                                    app=self.app,
                                    callback=self.process_task,
                                    logger=self.logger)
        self.scheduler = instantiate(self.eta_scheduler_cls,
                               precision=eta_scheduler_precision,
                               on_error=self.on_timer_error,
                               on_tick=self.on_timer_tick)

        self.beat = None
        if self.embed_clockservice:
            self.beat = beat.EmbeddedService(logger=self.logger,
                                    schedule_filename=self.schedule_filename)

        prefetch_count = self.concurrency * self.prefetch_multiplier
        self.listener = instantiate(self.listener_cls,
                                    self.ready_queue,
                                    self.scheduler,
                                    logger=self.logger,
                                    hostname=self.hostname,
                                    send_events=self.send_events,
                                    init_callback=self.ready_callback,
                                    initial_prefetch_count=prefetch_count,
                                    pool=self.pool,
                                    queues=self.queues,
                                    app=self.app)

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.components = filter(None, (self.pool,
                                        self.mediator,
                                        self.scheduler,
                                        self.beat,
                                        self.listener))

    def start(self):
        """Starts the workers main loop."""
        self._state = RUN

        for i, component in enumerate(self.components):
            self.logger.debug("Starting thread %s..." % (
                                    component.__class__.__name__))
            self._running = i + 1
            component.start()

    def process_task(self, wrapper):
        """Process task by sending it to the pool of workers."""
        try:
            try:
                wrapper.task.execute(wrapper, self.pool,
                    self.loglevel, self.logfile)
            except Exception, exc:
                self.logger.critical("Internal error %s: %s\n%s" % (
                                exc.__class__, exc, traceback.format_exc()))
        except (SystemExit, KeyboardInterrupt):
            self.stop()

    def stop(self):
        """Graceful shutdown of the worker server."""
        self._shutdown(warm=True)

    def terminate(self):
        """Not so graceful shutdown of the worker server."""
        self._shutdown(warm=False)

    def _shutdown(self, warm=True):
        """Gracefully shutdown the worker server."""
        what = (warm and "stopping" or "terminating").capitalize()

        if self._state != RUN or self._running != len(self.components):
            # Not fully started, can safely exit.
            return

        self._state = CLOSE
        signals.worker_shutdown.send(sender=self)

        for component in reversed(self.components):
            self.logger.debug("%s thread %s..." % (
                    what, component.__class__.__name__))
            stop = component.stop
            if not warm:
                stop = getattr(component, "terminate", stop)
            stop()

        self.listener.close_connection()
        self._state = TERMINATE

    def on_timer_error(self, exc_info):
        _, exc, _ = exc_info
        self.logger.error("Timer error: %r" % (exc, ))

    def on_timer_tick(self, delay):
        self.timer_debug("Scheduler wake-up! Next eta %s secs." % delay)

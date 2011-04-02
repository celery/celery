import socket
import logging
import traceback

from kombu.syn import blocking
from kombu.utils.finalize import Finalize

from celery import beat
from celery import concurrency as _concurrency
from celery import registry
from celery import platforms
from celery import signals
from celery.app import app_or_default
from celery.exceptions import SystemTerminate
from celery.log import SilenceRepeated
from celery.utils import noop, instantiate

from celery.worker import state
from celery.worker.buckets import TaskBucket, FastQueue

RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

#: List of signals to reset when a child process starts.
WORKER_SIGRESET = frozenset(["SIGTERM",
                             "SIGHUP",
                             "SIGTTIN",
                             "SIGTTOU",
                             "SIGUSR1"])

#: List of signals to ignore when a child process starts.
WORKER_SIGIGNORE = frozenset(["SIGINT"])


def process_initializer(app, hostname):
    """Initializes the process so it can be used to process tasks.

    Used for multiprocessing environments.

    """
    app = app_or_default(app)
    app.set_current()
    [platforms.reset_signal(signal) for signal in WORKER_SIGRESET]
    [platforms.ignore_signal(signal) for signal in WORKER_SIGIGNORE]
    platforms.set_mp_process_title("celeryd", hostname=hostname)

    # This is for windows and other platforms not supporting
    # fork(). Note that init_worker makes sure it's only
    # run once per process.
    app.loader.init_worker()

    signals.worker_process_init.send(sender=None)


class WorkController(object):
    """Unmanaged worker instance."""
    RUN = RUN
    CLOSE = CLOSE
    TERMINATE = TERMINATE

    #: The number of simultaneous processes doing work (default:
    #: :setting:`CELERYD_CONCURRENCY`)
    concurrency = None

    #: The loglevel used (default: :const:`logging.INFO`)
    loglevel = logging.ERROR

    #: The logfile used, if no logfile is specified it uses `stderr`
    #: (default: :setting:`CELERYD_LOG_FILE`).
    logfile = None

    #: If :const:`True`, celerybeat is embedded, running in the main worker
    #: process as a thread.
    embed_clockservice = None

    #: Enable the sending of monitoring events, these events can be captured
    #: by monitors (celerymon).
    send_events = False

    #: The :class:`logging.Logger` instance used for logging.
    logger = None

    #: The pool instance used.
    pool = None

    #: The internal queue object that holds tasks ready for immediate
    #: processing.
    ready_queue = None

    #: Instance of :class:`celery.worker.mediator.Mediator`.
    mediator = None

    #: Consumer instance.
    consumer = None

    _state = None
    _running = 0

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            send_events=None, hostname=None, ready_callback=noop,
            embed_clockservice=False, pool_cls=None, consumer_cls=None,
            mediator_cls=None, eta_scheduler_cls=None,
            schedule_filename=None, task_time_limit=None,
            task_soft_time_limit=None, max_tasks_per_child=None,
            pool_putlocks=None, db=None, prefetch_multiplier=None,
            eta_scheduler_precision=None, disable_rate_limits=None,
            autoscale=None, autoscaler_cls=None, scheduler_cls=None,
            app=None):

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
        self.pool_cls = _concurrency.get_implementation(
                            pool_cls or conf.CELERYD_POOL)
        self.consumer_cls = consumer_cls or conf.CELERYD_CONSUMER
        self.mediator_cls = mediator_cls or conf.CELERYD_MEDIATOR
        self.eta_scheduler_cls = eta_scheduler_cls or \
                                    conf.CELERYD_ETA_SCHEDULER

        self.autoscaler_cls = autoscaler_cls or \
                                    conf.CELERYD_AUTOSCALER
        self.schedule_filename = schedule_filename or \
                                    conf.CELERYBEAT_SCHEDULE_FILENAME
        self.scheduler_cls = scheduler_cls or conf.CELERYBEAT_SCHEDULER
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
        self._finalize = Finalize(self, self.stop, exitpriority=1)
        self._finalize_db = None

        if self.db:
            persistence = state.Persistent(self.db)
            self._finalize_db = Finalize(persistence, persistence.save,
                                         exitpriority=5)

        # Queues
        if self.disable_rate_limits:
            self.ready_queue = FastQueue()
            self.ready_queue.put = self.process_task
        else:
            self.ready_queue = TaskBucket(task_registry=registry.tasks)

        self.logger.debug("Instantiating thread components...")

        # Threads + Pool + Consumer
        self.autoscaler = None
        max_concurrency = None
        min_concurrency = concurrency
        if autoscale:
            max_concurrency, min_concurrency = autoscale

        self.pool = instantiate(self.pool_cls, min_concurrency,
                                logger=self.logger,
                                initializer=process_initializer,
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
                                precision=eta_scheduler_precision,
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
                                    app=self.app)

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.components = filter(None, (self.pool,
                                        self.mediator,
                                        self.scheduler,
                                        self.beat,
                                        self.autoscaler,
                                        self.consumer))

    def start(self):
        """Starts the workers main loop."""
        self._state = self.RUN

        try:
            for i, component in enumerate(self.components):
                self.logger.debug("Starting thread %s..." % (
                                        component.__class__.__name__))
                self._running = i + 1
                blocking(component.start)
        except SystemTerminate:
            self.terminate()
            raise SystemExit()
        except (SystemExit, KeyboardInterrupt), exc:
            self.stop()
            raise exc

    def process_task(self, request):
        """Process task by sending it to the pool of workers."""
        try:
            request.task.execute(request, self.pool,
                                 self.loglevel, self.logfile)
        except SystemTerminate:
            self.terminate()
            raise SystemExit()
        except (SystemExit, KeyboardInterrupt), exc:
            self.stop()
            raise exc
        except Exception, exc:
            self.logger.critical("Internal error %s: %s\n%s" % (
                            exc.__class__, exc, traceback.format_exc()))

    def stop(self, in_sighandler=False):
        """Graceful shutdown of the worker server."""
        if in_sighandler and not self.pool.signal_safe:
            return
        blocking(self._shutdown, warm=True)

    def terminate(self, in_sighandler=False):
        """Not so graceful shutdown of the worker server."""
        if in_sighandler and not self.pool.signal_safe:
            return
        blocking(self._shutdown, warm=False)

    def _shutdown(self, warm=True):
        what = (warm and "stopping" or "terminating").capitalize()

        if self._state != self.RUN or self._running != len(self.components):
            # Not fully started, can safely exit.
            return

        self._state = self.CLOSE
        signals.worker_shutdown.send(sender=self)

        for component in reversed(self.components):
            self.logger.debug("%s thread %s..." % (
                    what, component.__class__.__name__))
            stop = component.stop
            if not warm:
                stop = getattr(component, "terminate", stop)
            stop()

        self.consumer.close_connection()
        self._state = self.TERMINATE

    def on_timer_error(self, exc_info):
        _, exc, _ = exc_info
        self.logger.error("Timer error: %r" % (exc, ))

    def on_timer_tick(self, delay):
        self.timer_debug("Scheduler wake-up! Next eta %s secs." % delay)

"""

The Multiprocessing Worker Server

"""
import traceback
import logging
from Queue import Queue

from celery import conf
from celery import registry
from celery import platform
from celery import signals
from celery.log import setup_logger
from celery.beat import ClockServiceThread
from celery.worker.pool import TaskPool
from celery.worker.buckets import TaskBucket
from celery.worker.listener import CarrotListener
from celery.worker.scheduler import Scheduler
from celery.worker.controllers import Mediator, ScheduleController


def process_initializer():
    platform.set_mp_process_title("celeryd")


class WorkController(object):
    """Executes tasks waiting in the task queue.

    :param concurrency: see :attr:`concurrency`.
    :param logfile: see :attr:`logfile`.
    :param loglevel: see :attr:`loglevel`.
    :param embed_clockservice: see :attr:`run_clockservice`.
    :param send_events: see :attr:`send_events`.


    .. attribute:: concurrency

        The number of simultaneous processes doing work (default:
        :const:`celery.conf.DAEMON_CONCURRENCY`)

    .. attribute:: loglevel

        The loglevel used (default: :const:`logging.INFO`)

    .. attribute:: logfile

        The logfile used, if no logfile is specified it uses ``stderr``
        (default: :const:`celery.conf.DAEMON_LOG_FILE`).

    .. attribute:: embed_clockservice

        If ``True``, celerybeat is embedded, running in the main worker
        process as a thread.

    .. attribute:: send_events

        Enable the sending of monitoring events, these events can be captured
        by monitors (celerymon).

    .. attribute:: logger

        The :class:`logging.Logger` instance used for logging.

    .. attribute:: is_detached

        Flag describing if the worker is running as a daemon or not.

    .. attribute:: pool

        The :class:`multiprocessing.Pool` instance used.

    .. attribute:: ready_queue

        The :class:`Queue.Queue` that holds tasks ready for immediate
        processing.

    .. attribute:: hold_queue

        The :class:`Queue.Queue` that holds paused tasks. Reasons for holding
        back the task include waiting for ``eta`` to pass or the task is being
        retried.

    .. attribute:: schedule_controller

        Instance of :class:`celery.worker.controllers.ScheduleController`.

    .. attribute:: mediator

        Instance of :class:`celery.worker.controllers.Mediator`.

    .. attribute:: broker_listener

        Instance of :class:`CarrotListener`.

    """
    loglevel = logging.ERROR
    concurrency = conf.DAEMON_CONCURRENCY
    logfile = conf.DAEMON_LOG_FILE
    _state = None

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            send_events=conf.CELERY_SEND_EVENTS,
            is_detached=False, embed_clockservice=False):

        # Options
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.is_detached = is_detached
        self.logger = setup_logger(loglevel, logfile)
        self.embed_clockservice = embed_clockservice
        self.send_events = send_events

        # Queues
        if conf.DISABLE_RATE_LIMITS:
            self.ready_queue = Queue()
        else:
            self.ready_queue = TaskBucket(task_registry=registry.tasks)
        self.eta_scheduler = Scheduler(self.ready_queue)

        self.logger.debug("Instantiating thread components...")

        # Threads+Pool
        self.schedule_controller = ScheduleController(self.eta_scheduler,
                                                      logger=self.logger)
        self.pool = TaskPool(self.concurrency, logger=self.logger,
                             initializer=process_initializer)
        self.broker_listener = CarrotListener(self.ready_queue,
                                        self.eta_scheduler,
                                        logger=self.logger,
                                        send_events=send_events,
                                        initial_prefetch_count=concurrency)
        self.mediator = Mediator(self.ready_queue, self.safe_process_task,
                                 logger=self.logger)

        # Need a tight loop interval when embedded so the program
        # can be stopped in a sensible short time.
        self.clockservice = self.embed_clockservice and ClockServiceThread(
                                logger=self.logger,
                                is_detached=self.is_detached,
                                max_interval=1) or None

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.components = filter(None, (self.pool,
                                        self.mediator,
                                        self.schedule_controller,
                                        self.clockservice,
                                        self.broker_listener))

    def start(self):
        """Starts the workers main loop."""
        self._state = "RUN"

        try:
            for component in self.components:
                self.logger.debug("Starting thread %s..." % \
                        component.__class__.__name__)
                component.start()
        finally:
            self.stop()

    def safe_process_task(self, task):
        """Same as :meth:`process_task`, but catches all exceptions
        the task raises and log them as errors, to make sure the
        worker doesn't die."""
        try:
            try:
                self.process_task(task)
            except Exception, exc:
                self.logger.critical("Internal error %s: %s\n%s" % (
                                exc.__class__, exc, traceback.format_exc()))
        except (SystemExit, KeyboardInterrupt):
            self.stop()

    def process_task(self, task):
        """Process task by sending it to the pool of workers."""
        task.execute_using_pool(self.pool, self.loglevel, self.logfile)

    def stop(self):
        """Gracefully shutdown the worker server."""
        if self._state != "RUN":
            return

        signals.worker_shutdown.send(sender=self)

        [component.stop() for component in reversed(self.components)]

        self._state = "STOP"

"""

The Multiprocessing Worker Server

"""
from carrot.connection import DjangoBrokerConnection, AMQPConnectionException
from celery.worker.controllers import Mediator, PeriodicWorkController
from celery.worker.job import TaskWrapper
from celery.exceptions import NotRegistered
from celery.messaging import get_consumer_set
from celery.log import setup_logger
from celery.pool import TaskPool
from celery.utils import retry_over_time
from celery.datastructures import SharedCounter
from celery import registry
from celery import conf
from celery.buckets import TaskBucket
from Queue import Queue
import traceback
import logging
import socket


class AMQPListener(object):
    """Listen for messages received from the AMQP broker and
    move them the the bucket queue for task processing.

    :param bucket_queue: See :attr:`bucket_queue`.
    :param hold_queue: See :attr:`hold_queue`.

    .. attribute:: bucket_queue

        The queue that holds tasks ready for processing immediately.

    .. attribute:: hold_queue

        The queue that holds paused tasks. Reasons for being paused include
        a countdown/eta or that it's waiting for retry.

    .. attribute:: logger

        The logger used.

    """

    def __init__(self, bucket_queue, hold_queue, logger,
            initial_prefetch_count=2):
        self.amqp_connection = None
        self.task_consumer = None
        self.bucket_queue = bucket_queue
        self.hold_queue = hold_queue
        self.logger = logger
        self.prefetch_count = SharedCounter(initial_prefetch_count)

    def start(self):
        """Start the consumer.

        If the connection is lost, it tries to re-establish the connection
        over time and restart consuming messages.

        """

        while True:
            self.reset_connection()
            try:
                self.consume_messages()
            except (socket.error, AMQPConnectionException, IOError):
                self.logger.error("AMQPListener: Connection to broker lost. "
                                + "Trying to re-establish connection...")

    def consume_messages(self):
        """Consume messages forever (or until an exception is raised)."""
        task_consumer = self.task_consumer

        self.logger.debug("AMQPListener: Starting message consumer...")
        it = task_consumer.iterconsume(limit=None)

        self.logger.debug("AMQPListener: Ready to accept tasks!")

        while True:
            self.task_consumer.qos(prefetch_count=int(self.prefetch_count))
            it.next()

    def stop(self):
        """Stop processing AMQP messages and close the connection
        to the broker."""
        self.close_connection()

    def receive_message(self, message_data, message):
        """The callback called when a new message is received.

        If the message has an ``eta`` we move it to the hold queue,
        otherwise we move it the bucket queue for immediate processing.

        """
        try:
            task = TaskWrapper.from_message(message, message_data,
                                            logger=self.logger)
        except NotRegistered, exc:
            self.logger.error("Unknown task ignored: %s" % (exc))
            return

        eta = message_data.get("eta")
        if eta:
            self.prefetch_count.increment()
            self.logger.info("Got task from broker: %s[%s] eta:[%s]" % (
                    task.task_name, task.task_id, eta))
            self.hold_queue.put((task, eta, self.prefetch_count.decrement))
        else:
            self.logger.info("Got task from broker: %s[%s]" % (
                    task.task_name, task.task_id))
            self.bucket_queue.put(task)

    def close_connection(self):
        """Close the AMQP connection."""
        if self.task_consumer:
            self.task_consumer.close()
            self.task_consumer = None
        if self.amqp_connection:
            self.logger.debug(
                    "AMQPListener: Closing connection to the broker...")
            self.amqp_connection.close()
            self.amqp_connection = None

    def reset_connection(self):
        """Reset the AMQP connection, and reinitialize the
        :class:`carrot.messaging.ConsumerSet` instance.

        Resets the task consumer in :attr:`task_consumer`.

        """
        self.logger.debug(
                "AMQPListener: Re-establishing connection to the broker...")
        self.close_connection()
        self.amqp_connection = self._open_connection()
        self.task_consumer = get_consumer_set(connection=self.amqp_connection)
        self.task_consumer.register_callback(self.receive_message)

    def _open_connection(self):
        """Retries connecting to the AMQP broker over time.

        See :func:`celery.utils.retry_over_time`.

        """

        def _connection_error_handler(exc, interval):
            """Callback handler for connection errors."""
            self.logger.error("AMQP Listener: Connection Error: %s. " % exc
                     + "Trying again in %d seconds..." % interval)

        def _establish_connection():
            """Establish a connection to the AMQP broker."""
            conn = DjangoBrokerConnection()
            connected = conn.connection # Connection is established lazily.
            return conn

        if not conf.AMQP_CONNECTION_RETRY:
            return _establish_connection()

        conn = retry_over_time(_establish_connection, (socket.error, IOError),
                               errback=_connection_error_handler,
                               max_retries=conf.AMQP_CONNECTION_MAX_RETRIES)
        self.logger.debug("AMQPListener: Connection Established.")
        return conn


class WorkController(object):
    """Executes tasks waiting in the task queue.

    :param concurrency: see :attr:`concurrency`.
    :param logfile: see :attr:`logfile`.
    :param loglevel: see :attr:`loglevel`.


    .. attribute:: concurrency

        The number of simultaneous processes doing work (default:
        :const:`celery.conf.DAEMON_CONCURRENCY`)

    .. attribute:: loglevel

        The loglevel used (default: :const:`logging.INFO`)

    .. attribute:: logfile

        The logfile used, if no logfile is specified it uses ``stderr``
        (default: :const:`celery.conf.DAEMON_LOG_FILE`).

    .. attribute:: logger

        The :class:`logging.Logger` instance used for logging.

    .. attribute:: is_detached

        Flag describing if the worker is running as a daemon or not.

    .. attribute:: pool

        The :class:`multiprocessing.Pool` instance used.

    .. attribute:: bucket_queue

        The :class:`Queue.Queue` that holds tasks ready for immediate
        processing.

    .. attribute:: hold_queue

        The :class:`Queue.Queue` that holds paused tasks. Reasons for holding
        back the task include waiting for ``eta`` to pass or the task is being
        retried.

    .. attribute:: periodic_work_controller

        Instance of :class:`celery.worker.controllers.PeriodicWorkController`.

    .. attribute:: mediator

        Instance of :class:`celery.worker.controllers.Mediator`.

    .. attribute:: amqp_listener

        Instance of :class:`AMQPListener`.

    """
    loglevel = logging.ERROR
    concurrency = conf.DAEMON_CONCURRENCY
    logfile = conf.DAEMON_LOG_FILE
    _state = None

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            is_detached=False):

        # Options
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.is_detached = is_detached
        self.logger = setup_logger(loglevel, logfile)

        # Queues
        if conf.DISABLE_RATE_LIMITS:
            self.bucket_queue = Queue()
        else:
            self.bucket_queue = TaskBucket(task_registry=registry.tasks)
        self.hold_queue = Queue()

        self.logger.debug("Instantiating thread components...")

        # Threads+Pool
        self.periodic_work_controller = PeriodicWorkController(
                                                    self.bucket_queue,
                                                    self.hold_queue)
        self.pool = TaskPool(self.concurrency, logger=self.logger)
        self.amqp_listener = AMQPListener(self.bucket_queue, self.hold_queue,
                                          logger=self.logger,
                                          initial_prefetch_count=concurrency)
        self.mediator = Mediator(self.bucket_queue, self.safe_process_task)

        # The order is important here;
        #   the first in the list is the first to start,
        # and they must be stopped in reverse order.
        self.components = [self.pool,
                           self.mediator,
                           self.periodic_work_controller,
                           self.amqp_listener]

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
        # shut down the periodic work controller thread
        if self._state != "RUN":
            return

        [component.stop() for component in reversed(self.components)]

        self._state = "STOP"

"""

The Multiprocessing Worker Server

Documentation for this module is in ``docs/reference/celery.worker.rst``.

"""
from carrot.connection import DjangoAMQPConnection
from celery.worker.controllers import Mediator, PeriodicWorkController
from celery.worker.job import TaskWrapper, UnknownTaskError
from celery.messaging import TaskConsumer
from celery.conf import DAEMON_CONCURRENCY, DAEMON_LOG_FILE
from celery.log import setup_logger
from celery.pool import TaskPool
from Queue import Queue
import traceback
import logging


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

    def __init__(self, bucket_queue, hold_queue, logger):
        self.amqp_connection = None
        self.task_consumer = None
        self.bucket_queue = bucket_queue
        self.hold_queue = hold_queue
        self.logger = logger

    def start(self):
        """Start processing AMQP messages."""
        task_consumer = self.reset_connection()
        it = task_consumer.iterconsume(limit=None)
        
        while True:
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
        task = TaskWrapper.from_message(message, message_data,
                                        logger=self.logger)
        eta = message_data.get("eta")
        if eta:
           self.hold_queue.put((task, eta))
        else:
            self.bucket_queue.put(task)

    def close_connection(self):
        """Close the AMQP connection."""
        if self.task_consumer:
            self.task_consumer.close()
        if self.amqp_connection:
            self.amqp_connection.close()

    def reset_connection(self):
        """Reset the AMQP connection, and reinitialize the
        :class:`celery.messaging.TaskConsumer` instance.

        Resets the task consumer in :attr:`task_consumer`.

        """
        self.close_connection()
        self.amqp_connection = DjangoAMQPConnection()
        self.task_consumer = TaskConsumer(connection=self.amqp_connection)
        self.task_consumer.register_callback(self.receive_message)
        return self.task_consumer


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
    concurrency = DAEMON_CONCURRENCY
    logfile = DAEMON_LOG_FILE
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
        self.bucket_queue = Queue()
        self.hold_queue = Queue()

        # Threads+Pool
        self.periodic_work_controller = PeriodicWorkController(
                                                    self.bucket_queue,
                                                    self.hold_queue)
        self.pool = TaskPool(self.concurrency, logger=self.logger)
        self.amqp_listener = AMQPListener(self.bucket_queue, self.hold_queue,
                                          logger=self.logger)
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
            [component.start() for component in self.components]
        finally:
            self.stop()

    def safe_process_task(self, task):
        """Same as :meth:`process_task`, but catches all exceptions
        the task raises and log them as errors, to make sure the
        worker doesn't die."""
        try:
            try:
                self.process_task(task)
            except ValueError:
                # execute_next_task didn't return a r/name/id tuple,
                # probably because it got an exception.
                pass
            except UnknownTaskError, exc:
                self.logger.info("Unknown task ignored: %s" % (exc))
            except Exception, exc:
                self.logger.critical("Message queue raised %s: %s\n%s" % (
                                exc.__class__, exc, traceback.format_exc()))
        except (SystemExit, KeyboardInterrupt):
            self.stop()

    def process_task(self, task):
        """Process task by sending it to the pool of workers."""
        self.logger.info("Got task from broker: %s[%s]" % (
                task.task_name, task.task_id))
        task.execute_using_pool(self.pool, self.loglevel, self.logfile)
        self.logger.debug("Task %s has been executed." % task)

    def stop(self):
        """Gracefully shutdown the worker server."""
        # shut down the periodic work controller thread
        if self._state != "RUN":
            return

        [component.stop() for component in reversed(self.components)]

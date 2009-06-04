"""celery.worker"""
from carrot.connection import DjangoAMQPConnection
from celery.messaging import TaskConsumer
from celery.conf import DAEMON_CONCURRENCY, DAEMON_LOG_FILE
from celery.conf import QUEUE_WAKEUP_AFTER, EMPTY_MSG_EMIT_EVERY
from celery.log import setup_logger
from celery.registry import tasks
from celery.datastructures import TaskProcessQueue
from celery.models import PeriodicTaskMeta
from celery.backends import default_backend, default_periodic_status_backend
from celery.timer import EventTimer
import multiprocessing
import simplejson
import traceback
import logging
import time


class EmptyQueue(Exception):
    """The message queue is currently empty."""


class UnknownTask(Exception):
    """Got an unknown task in the queue. The message is requeued and
    ignored."""


def jail(task_id, func, args, kwargs):
    """Wraps the task in a jail, which catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to ``"DONE"``.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to ``"FAILURE"``.

    :param task_id: The id of the task.
    :param func: Callable object to execute.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :returns: the function return value on success, or
        the exception instance on failure.

    """
    try:
        result = func(*args, **kwargs)
    except Exception, exc:
        default_backend.mark_as_failure(task_id, exc)
        return exc
    else:
        default_backend.mark_as_done(task_id, result)
        return result


class TaskWrapper(object):
    """Class wrapping a task to be run.

    :param task_name: see :attr:`task_name`.

    :param task_id: see :attr:`task_id`.

    :param task_func: see :attr:`task_func`

    :param args: see :attr:`args`

    :param kwargs: see :attr:`kwargs`.

    .. attribute:: task_name

        Kind of task. Must be a name registered in the task registry.

    .. attribute:: task_id

        UUID of the task.

    .. attribute:: task_func

        The tasks callable object.

    .. attribute:: args

        List of positional arguments to apply to the task.

    .. attribute:: kwargs

        Mapping of keyword arguments to apply to the task.

    """

    def __init__(self, task_name, task_id, task_func, args, kwargs):
        self.task_name = task_name
        self.task_id = task_id
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        return '<%s: {name:"%s", id:"%s", args:"%s", kwargs:"%s"}>' % (
                self.__class__.__name__,
                self.task_name, self.task_id,
                self.args, self.kwargs)

    @classmethod
    def from_message(cls, message):
        """Create a :class:`TaskWrapper` from a task message sent by
        :class:`celery.messaging.TaskPublisher`.

        :raises UnknownTask: if the message does not describe a task,
            the message is also rejected.

        :returns: :class:`TaskWrapper` instance.

        """
        message_data = message.decode()
        task_name = message_data["task"]
        task_id = message_data["id"]
        args = message_data["args"]
        kwargs = message_data["kwargs"]
        if task_name not in tasks:
            message.reject()
            raise UnknownTask(task_name)
        task_func = tasks[task_name]
        return cls(task_name, task_id, task_func, args, kwargs)

    def extend_with_default_kwargs(self, loglevel, logfile):
        """Extend the tasks keyword arguments with standard task arguments.

        These are ``logfile``, ``loglevel``, ``task_id`` and ``task_name``.

        """
        task_func_kwargs = {"logfile": logfile,
                            "loglevel": loglevel,
                            "task_id": self.task_id,
                            "task_name": self.task_name}
        task_func_kwargs.update(self.kwargs)
        return task_func_kwargs

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :func:`jail` and store return value
        and status in the task meta backend.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        """
        task_func_kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        return jail(self.task_id, [
                        self.task_func, self.args, task_func_kwargs])

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using the :mod:`multiprocessing` pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        :returns :class:`multiprocessing.AsyncResult` instance.

        """
        task_func_kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        jail_args = [self.task_id, self.task_func,
                     self.args, task_func_kwargs]
        return pool.apply_async(jail, jail_args, {},
                                self.task_name, self.task_id)


class WorkController(object):
    """Executes tasks waiting in the task queue.

    :param concurrency: see :attr:`concurrency`.

    :param logfile: see :attr:`logfile`.

    :param loglevel: see :attr:`loglevel`.

    :param queue_wakeup_after: see :attr:`queue_wakeup_after`.


    .. attribute:: concurrency

        The number of simultaneous processes doing work (default:
        :const:`celery.conf.DAEMON_CONCURRENCY`)

    .. attribute:: loglevel

        The loglevel used (default: :const:`logging.INFO`)

    .. attribute:: logfile

        The logfile used, if no logfile is specified it uses ``stderr``
        (default: :const:`celery.conf.DAEMON_LOG_FILE`).

    .. attribute:: queue_wakeup_after

        The time it takes for the daemon to wake up after the queue is empty,
        so it can check for more work
        (default: :const:`celery.conf.QUEUE_WAKEUP_AFTER`).

    .. attribute:: empty_msg_emit_every

        How often the daemon emits the ``"Waiting for queue..."`` message.
        If this is ``None``, the message will never be logged.
        (default: :const:`celery.conf.EMPTY_MSG_EMIT_EVERY`)

    .. attribute:: logger

        The :class:`logging.Logger` instance used for logging.

    .. attribute:: pool

        The :class:`multiprocessing.Pool` instance used.

    .. attribute:: task_consumer

        The :class:`celery.messaging.TaskConsumer` instance used.

    """
    loglevel = logging.ERROR
    concurrency = DAEMON_CONCURRENCY
    logfile = DAEMON_LOG_FILE
    queue_wakeup_after = QUEUE_WAKEUP_AFTER
    empty_msg_emit_every = EMPTY_MSG_EMIT_EVERY

    def __init__(self, concurrency=None, logfile=None, loglevel=None,
            queue_wakeup_after=None, is_detached=False):
        self.loglevel = loglevel or self.loglevel
        self.concurrency = concurrency or self.concurrency
        self.logfile = logfile or self.logfile
        self.queue_wakeup_after = queue_wakeup_after or \
                                    self.queue_wakeup_after
        self.logger = setup_logger(loglevel, logfile)
        self.pool = TaskProcessQueue(self.concurrency, logger=self.logger,
                done_msg="Task %(name)s[%(id)s] processed: %(return_value)s")
        self.task_consumer = None
        self.is_detached = is_detached
        self.reset_connection()

    def reset_connection(self):
        """Reset the AMQP connection, and reinitialize the
        :class:`celery.messaging.TaskConsumer` instance.

        Resets the task consumer in :attr:`task_consumer`.

        """
        if self.task_consumer:
            self.task_consumer.connection.close()
        amqp_connection = DjangoAMQPConnection()
        self.task_consumer = TaskConsumer(connection=amqp_connection)

    def connection_diagnostics(self):
        """Diagnose the AMQP connection, and reset connection if
        necessary."""
        if hasattr(self.task_consumer, "backend"):
            connection = self.task_consumer.backend.channel.connection
        else:
            connection = self.task_consumer.channel.connection

        if not connection:
            self.logger.info(
                    "AMQP Connection has died, restoring connection.")
            self.reset_connection()

    def receive_message(self):
        """Receive the next message from the message broker.

        Tries to reset the AMQP connection if not available.
        Returns ``None`` if no message is waiting on the queue.

        :rtype: :class:`carrot.messaging.Message` instance.

        """
        #self.connection_diagnostics()
        self.logger.debug("Trying to fetch message from broker...")
        message = self.task_consumer.fetch()
        if message is not None:
            self.logger.debug("Acknowledging message with delivery tag %s" % (
                message.delivery_tag))
            message.ack()
        return message

    def fetch_next_task(self):
        """Fetch the next task from the AMQP broker.

        Raises :exc:`EmptyQueue` exception if there is no message
        waiting on the queue.

        :returns: :class:`TaskWrapper` instance.

        """
        message = self.receive_message()
        if message is None: # No messages waiting.
            raise EmptyQueue()

        task = TaskWrapper.from_message(message)
        self.logger.info("Got task from broker: %s[%s]" % (
                            task.task_name, task.task_id))

        return task, message

    def execute_next_task(self):
        """Execute the next task on the queue using the multiprocessing pool.

        Catches all exceptions and logs them with level
        :const:`logging.CRITICAL`.

        """
        self.logger.debug("Trying to fetch a task.")
        task, message = self.fetch_next_task()
        self.logger.debug("Got a task: %s. Trying to execute it..." % task)

        result = task.execute_using_pool(self.pool, self.loglevel,
                                         self.logfile)

        self.logger.debug("Task %s has been executed asynchronously." % task)

        return result, task.task_name, task.task_id

    def run_periodic_tasks(self):
        """Schedule all waiting periodic tasks for execution.

        """
        self.logger.debug("Looking for periodic tasks ready for execution...")
        default_periodic_status_backend.run_periodic_tasks()

    def schedule_retry_tasks(self):
        """Reschedule all requeued tasks waiting for retry."""
        pass

    def run(self):
        """Starts the workers main loop."""
        log_wait = lambda: self.logger.info("Waiting for queue...")
        ev_msg_waiting = EventTimer(log_wait, self.empty_msg_emit_every)
        events = [
            EventTimer(self.run_periodic_tasks, 1),
            EventTimer(self.schedule_retry_tasks, 2),
        ]

        # If not running as daemon, and DEBUG logging level is enabled,
        # print pool PIDs and sleep for a second before we start.
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Pool child processes: [%s]" % (
                "|".join(map(str, self.pool.get_worker_pids()))))
            if not self.is_detached:
                time.sleep(1)

        while True:
            [event.tick() for event in events]
            try:
                result, task_name, task_id = self.execute_next_task()
            except ValueError:
                # execute_next_task didn't return a r/name/id tuple,
                # probably because it got an exception.
                continue
            except EmptyQueue:
                ev_msg_waiting.tick()
                time.sleep(self.queue_wakeup_after)
                continue
            except UnknownTask, e:
                self.logger.info("Unknown task ignored: %s" % (e))
                continue
            except Exception, e:
                self.logger.critical("Message queue raised %s: %s\n%s" % (
                             e.__class__, e, traceback.format_exc()))
                continue

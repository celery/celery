"""celery.worker"""
from carrot.connection import DjangoAMQPConnection
from celery.messaging import TaskConsumer
from celery.conf import DAEMON_CONCURRENCY, DAEMON_LOG_FILE
from celery.conf import QUEUE_WAKEUP_AFTER, EMPTY_MSG_EMIT_EVERY
from celery.conf import SEND_CELERY_TASK_ERROR_EMAILS
from celery.log import setup_logger
from celery.registry import tasks
from celery.pool import TaskPool
from celery.datastructures import ExceptionInfo
from celery.backends import default_backend, default_periodic_status_backend
from celery.timer import EventTimer
from django.core.mail import mail_admins
from celery.monitoring import TaskTimerStats
import multiprocessing
import traceback
import threading
import logging
import socket
import time
import sys


# pep8.py borks on a inline signature separator and
# says "trailing whitespace" ;)
EMAIL_SIGNATURE_SEP = "-- "
TASK_FAIL_EMAIL_BODY = """
Task %%(name)s with id %%(id)s raised exception: %%(exc)s

The contents of the full traceback was:

%%(traceback)s

%%(EMAIL_SIGNATURE_SEP)s
Just thought I'd let you know!
celeryd at %%(hostname)s.
""" % {"EMAIL_SIGNATURE_SEP": EMAIL_SIGNATURE_SEP}


class EmptyQueue(Exception):
    """The message queue is currently empty."""


class UnknownTask(Exception):
    """Got an unknown task in the queue. The message is requeued and
    ignored."""


def jail(task_id, task_name, func, args, kwargs):
    """Wraps the task in a jail, which catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to ``"DONE"``.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to ``"FAILURE"``.

    :param task_id: The id of the task.
    :param task_name: The name of the task.
    :param func: Callable object to execute.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :returns: the function return value on success, or
        the exception instance on failure.

    """
    timer_stat = TaskTimerStats.start(task_id, task_name, args, kwargs)

    # See: http://groups.google.com/group/django-users/browse_thread/
    #       thread/78200863d0c07c6d/38402e76cf3233e8?hl=en&lnk=gst&
    #       q=multiprocessing#38402e76cf3233e8
    from django.db import connection
    connection.close()

    # Reset cache connection only if using memcached/libmemcached
    from django.core import cache
    # XXX At Opera we use a custom memcached backend that uses libmemcached
    # instead of libmemcache (cmemcache). Should find a better solution for
    # this, but for now "memcached" should probably be unique enough of a
    # string to not make problems.
    cache_backend = cache.settings.CACHE_BACKEND
    if hasattr(cache, "parse_backend_uri"):
        cache_scheme = cache.parse_backend_uri(cache_backend)[0]
    else:
        # Django <= 1.0.2
        cache_scheme = cache_backend.split(":", 1)[0]
    if "memcached" in cache_scheme:
        cache.cache.close()

    # Backend process cleanup
    default_backend.process_cleanup()

    # Convert any unicode keys in the keyword arguments to ascii.
    kwargs = dict([(k.encode("utf-8"), v)
                        for k, v in kwargs.items()])
    try:
        result = func(*args, **kwargs)
    except (SystemExit, KeyboardInterrupt):
        raise
    except Exception, exc:
        default_backend.mark_as_failure(task_id, exc)
        retval = ExceptionInfo(sys.exc_info())
    else:
        default_backend.mark_as_done(task_id, result)
        retval = result
    finally:
        timer_stat.stop()

    return retval

    

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
    success_msg = "Task %(name)s[%(id)s] processed: %(return_value)s"
    fail_msg = """
        Task %(name)s[%(id)s] raised exception: %(exc)s\n%(traceback)s
    """
    fail_email_subject = """
        [celery@%(hostname)s] Error: Task %(name)s (%(id)s): %(exc)s
    """
    fail_email_body = TASK_FAIL_EMAIL_BODY

    def __init__(self, task_name, task_id, task_func, args, kwargs, **opts):
        self.task_name = task_name
        self.task_id = task_id
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs
        self.logger = kwargs.get("logger")
        for opt in ("success_msg", "fail_msg", "fail_email_subject",
                "fail_email_body"):
            setattr(self, opt, opts.get(opt, getattr(self, opt, None)))
        if not self.logger:
            self.logger = multiprocessing.get_logger()

    def __repr__(self):
        return '<%s: {name:"%s", id:"%s", args:"%s", kwargs:"%s"}>' % (
                self.__class__.__name__,
                self.task_name, self.task_id,
                self.args, self.kwargs)

    @classmethod
    def from_message(cls, message, logger):
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
            raise UnknownTask(task_name)
        task_func = tasks[task_name]
        return cls(task_name, task_id, task_func, args, kwargs, logger=logger)

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
        return jail(self.task_id, self.task_name, [
                        self.task_func, self.args, task_func_kwargs])

    def on_success(self, ret_value, meta):
        """The handler used if the task was successfully processed (
        without raising an exception)."""
        task_id = meta.get("task_id")
        task_name = meta.get("task_name")
        msg = self.success_msg.strip() % {
                "id": task_id,
                "name": task_name,
                "return_value": ret_value}
        self.logger.info(msg)

    def on_failure(self, exc_info, meta):
        """The handler used if the task raised an exception."""
        task_id = meta.get("task_id")
        task_name = meta.get("task_name")
        context = {
            "hostname": socket.gethostname(),
            "id": task_id,
            "name": task_name,
            "exc": exc_info.exception,
            "traceback": exc_info.traceback,
        }
        self.logger.error(self.fail_msg.strip() % context)

        if SEND_CELERY_TASK_ERROR_EMAILS:
            subject = self.fail_email_subject.strip() % context
            body = self.fail_email_body.strip() % context
            mail_admins(subject, body, fail_silently=True)

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using the :mod:`multiprocessing` pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        :returns :class:`multiprocessing.AsyncResult` instance.

        """
        task_func_kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        jail_args = [self.task_id, self.task_name, self.task_func,
                     self.args, task_func_kwargs]
        return pool.apply_async(jail, args=jail_args,
                callbacks=[self.on_success], errbacks=[self.on_failure],
                meta={"task_id": self.task_id, "task_name": self.task_name})


class PeriodicWorkController(threading.Thread):
    """A thread that continuously checks if there are
    :class:`celery.task.PeriodicTask`s waiting for execution, and executes
    them.

    Example

        >>> PeriodicWorkController().start()
    
    """
    
    def __init__(self):
        super(PeriodicWorkController, self).__init__()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
    
    def run(self):
        """Don't use :meth:`run`. use :meth:`start`."""
        while True:
            if self._shutdown.isSet():
                break
            default_periodic_status_backend.run_periodic_tasks()
            time.sleep(1)
        self._stopped.set() # indicate that we are stopped
    
    def stop(self):
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done


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
        self.pool = TaskPool(self.concurrency, logger=self.logger)
        self.periodicworkcontroller = PeriodicWorkController()
        self.task_consumer = None
        self.task_consumer_it = None
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
        self.task_consumer_it = self.task_consumer.iterqueue(infinite=True)

    def connection_diagnostics(self):
        """Diagnose the AMQP connection, and reset connection if
        necessary."""
        connection = self.task_consumer.backend.channel.connection

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
        message = self.task_consumer_it.next()
        if not message:
            raise EmptyQueue()
        return message

    def process_task(self, message):
        """Process task message by passing it to the pool of workers."""
        task = TaskWrapper.from_message(message, logger=self.logger)
        self.logger.info("Got task from broker: %s[%s]" % (
            task.task_name, task.task_id))
        self.logger.debug("Got a task: %s. Trying to execute it..." % task)

        result = task.execute_using_pool(self.pool, self.loglevel,
                                         self.logfile)

        self.logger.debug("Task %s has been executed asynchronously." % task)

        return result

    def execute_next_task(self):
        """Execute the next task on the queue using the multiprocessing pool.

        Catches all exceptions and logs them with level
        :const:`logging.CRITICAL`.

        Raises :exc:`EmptyQueue` exception if there is no message
        waiting on the queue.

        """
        self.process_task(self.receive_message())

    def schedule_retry_tasks(self):
        """Reschedule all requeued tasks waiting for retry."""
        pass
    
    def shutdown(self):
        # shut down the periodic work controller thread
        self.periodicworkcontroller.stop()
        self.pool.terminate()

    def run(self):
        """Starts the workers main loop."""
        log_wait = lambda: self.logger.debug("Waiting for queue...")
        ev_msg_waiting = EventTimer(log_wait, self.empty_msg_emit_every)

        self.pool.run()
        self.periodicworkcontroller.start()

        # If not running as daemon, and DEBUG logging level is enabled,
        # print pool PIDs and sleep for a second before we start.
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("Pool child processes: [%s]" % (
                "|".join(map(str, self.pool.get_worker_pids()))))
            if not self.is_detached:
                time.sleep(1)
        
        try:
            while True:
                try:
                    self.execute_next_task()
                except ValueError:
                    # execute_next_task didn't return a r/name/id tuple,
                    # probably because it got an exception.
                    continue
                except EmptyQueue:
                    ev_msg_waiting.tick()
                    time.sleep(self.queue_wakeup_after)
                    continue
                except UnknownTask, exc:
                    self.logger.info("Unknown task ignored: %s" % (exc))
                    continue
                except Exception, exc:
                    self.logger.critical("Message queue raised %s: %s\n%s" % (
                                 exc.__class__, exc, traceback.format_exc()))
                    continue
                except:
                    self.shutdown()
                    raise
        except (SystemExit, KeyboardInterrupt):
            self.shutdown()

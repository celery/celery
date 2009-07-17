"""

Jobs Executable by the Worker Server.

"""
from celery.registry import tasks, NotRegistered
from celery.datastructures import ExceptionInfo
from celery.backends import default_backend
from django.core.mail import mail_admins
from celery.monitoring import TaskTimerStats
import multiprocessing
import traceback
import socket
import sys


# pep8.py borks on a inline signature separator and
# says "trailing whitespace" ;)
EMAIL_SIGNATURE_SEP = "-- "
TASK_FAIL_EMAIL_BODY = """
Task %%(name)s with id %%(id)s raised exception: %%(exc)s


Task was called with args:%%(args)s kwargs:%%(kwargs)s.
The contents of the full traceback was:

%%(traceback)s

%(EMAIL_SIGNATURE_SEP)s
Just thought I'd let you know!
celeryd at %%(hostname)s.
""" % {"EMAIL_SIGNATURE_SEP": EMAIL_SIGNATURE_SEP}


def jail(task_id, task_name, func, args, kwargs, on_acknowledge=None):
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
    ignore_result = getattr(func, "ignore_result", False)
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

    # Handle task acknowledgement.
    if on_acknowledge:
        on_acknowledge()

    try:
        result = func(*args, **kwargs)
    except (SystemExit, KeyboardInterrupt):
        raise
    except Exception, exc:
        default_backend.mark_as_failure(task_id, exc)
        retval = ExceptionInfo(sys.exc_info())
    else:
        if not ignore_result:
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

    .. attribute:: message

        The original message sent. Used for acknowledging the message.

    """
    success_msg = "Task %(name)s[%(id)s] processed: %(return_value)s"
    fail_msg = """
        Task %(name)s[%(id)s] raised exception: %(exc)s\n%(traceback)s
    """
    fail_email_subject = """
        [celery@%(hostname)s] Error: Task %(name)s (%(id)s): %(exc)s
    """
    fail_email_body = TASK_FAIL_EMAIL_BODY

    def __init__(self, task_name, task_id, task_func, args, kwargs,
            on_acknowledge=None, **opts):
        self.task_name = task_name
        self.task_id = task_id
        self.task_func = task_func
        self.args = args
        self.kwargs = kwargs
        self.logger = kwargs.get("logger")
        self.on_acknowledge = on_acknowledge
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
    def from_message(cls, message, message_data, logger=None):
        """Create a :class:`TaskWrapper` from a task message sent by
        :class:`celery.messaging.TaskPublisher`.

        :raises UnknownTaskError: if the message does not describe a task,
            the message is also rejected.

        :returns: :class:`TaskWrapper` instance.

        """
        task_name = message_data["task"]
        task_id = message_data["id"]
        args = message_data["args"]
        kwargs = message_data["kwargs"]

        # Convert any unicode keys in the keyword arguments to ascii.
        kwargs = dict((key.encode("utf-8"), value)
                        for key, value in kwargs.items())

        if task_name not in tasks:
            raise NotRegistered(task_name)
        task_func = tasks[task_name]
        return cls(task_name, task_id, task_func, args, kwargs,
                    on_acknowledge=message.ack, logger=logger)

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
        return jail(self.task_id, self.task_name, self.task_func,
                    self.args, task_func_kwargs, self.on_acknowledge)

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
        from celery.conf import SEND_CELERY_TASK_ERROR_EMAILS

        task_id = meta.get("task_id")
        task_name = meta.get("task_name")
        context = {
            "hostname": socket.gethostname(),
            "id": task_id,
            "name": task_name,
            "exc": exc_info.exception,
            "traceback": exc_info.traceback,
            "args": self.args,
            "kwargs": self.kwargs,
        }
        self.logger.error(self.fail_msg.strip() % context)

        task_obj = tasks.get(task_name, object)
        send_error_email = SEND_CELERY_TASK_ERROR_EMAILS and not \
                getattr(task_obj, "disable_error_emails", False)
        if send_error_email:
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
                     self.args, task_func_kwargs, self.on_acknowledge]
        return pool.apply_async(jail, args=jail_args,
                callbacks=[self.on_success], errbacks=[self.on_failure],
                meta={"task_id": self.task_id, "task_name": self.task_name})

"""

Jobs Executable by the Worker Server.

"""
from celery.registry import tasks
from celery.exceptions import NotRegistered
from celery.execute import TaskTrace
from celery.utils import noop, fun_takes_kwargs
from celery.log import get_default_logger
from celery.monitoring import TaskTimerStats
from django.core.mail import mail_admins
from celery.loaders import current_loader
from celery.backends import default_backend
from celery.datastructures import ExceptionInfo
import sys
import socket
import warnings

# pep8.py borks on a inline signature separator and
# says "trailing whitespace" ;)
EMAIL_SIGNATURE_SEP = "-- "
TASK_FAIL_EMAIL_BODY = """
Task %%(name)s with id %%(id)s raised exception: %%(exc)s


Task was called with args: %%(args)s kwargs: %%(kwargs)s.

The contents of the full traceback was:

%%(traceback)s

%(EMAIL_SIGNATURE_SEP)s
Just to let you know,
celeryd at %%(hostname)s.
""" % {"EMAIL_SIGNATURE_SEP": EMAIL_SIGNATURE_SEP}


class AlreadyExecutedError(Exception):
    """Tasks can only be executed once, as they might change
    world-wide state."""


class WorkerTaskTrace(TaskTrace):
    """Wraps the task in a jail, catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to ``"SUCCESS"``.

    If the call raises :exc:`celery.exceptions.RetryTaskError`, it extracts
    the original exception, uses that as the result and sets the task status
    to ``"RETRY"``.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to ``"FAILURE"``.

    :param task_name: The name of the task to execute.
    :param task_id: The unique id of the task.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :returns: the function return value on success, or
        the exception instance on failure.

    """

    def __init__(self, *args, **kwargs):
        self.backend = kwargs.pop("backend", default_backend)
        self.loader = kwargs.pop("loader", current_loader)
        super(WorkerTaskTrace, self).__init__(*args, **kwargs)

    def execute_safe(self, *args, **kwargs):
        try:
            return self.execute(*args, **kwargs)
        except Exception, exc:
            type_, value_, tb = sys.exc_info()
            exc = self.backend.prepare_exception(exc)
            warnings.warn("Exception happend outside of task body: %s: %s" % (
                str(exc.__class__), str(exc)))
            return ExceptionInfo((type_, exc, tb))

    def execute(self):
        # Run task loader init handler.
        self.loader.on_task_init(self.task_id, self.task)

        # Backend process cleanup
        self.backend.process_cleanup()

        timer_stat = TaskTimerStats.start(self.task_id, self.task_name,
                                          self.args, self.kwargs)
        try:
            return self._trace()
        finally:
            timer_stat.stop()

    def handle_success(self, retval, *args):
        """Handle successful execution.

        Saves the result to the current result store (skipped if the task's
            ``ignore_result`` attribute is set to ``True``).

        """
        if not self.task.ignore_result:
            self.backend.mark_as_done(self.task_id, retval)
        return super(WorkerTaskTrace, self).handle_success(retval, *args)

    def handle_retry(self, exc, type_, tb, strtb):
        """Handle retry exception."""
        message, orig_exc = exc.args
        self.backend.mark_as_retry(self.task_id, orig_exc, strtb)
        return super(WorkerTaskTrace, self).handle_retry(exc, type_,
                                                         tb, strtb)

    def handle_failure(self, exc, type_, tb, strtb):
        """Handle exception."""
        # mark_as_failure returns an exception that is guaranteed to
        # be pickleable.
        stored_exc = self.backend.mark_as_failure(self.task_id, exc, strtb)
        return super(WorkerTaskTrace, self).handle_failure(
                stored_exc, type_, tb, strtb)


def execute_and_trace(*args, **kwargs):
    return WorkerTaskTrace(*args, **kwargs).execute_safe()


class TaskWrapper(object):
    """Class wrapping a task to be passed around and finally
    executed inside of the worker.

    :param task_name: see :attr:`task_name`.

    :param task_id: see :attr:`task_id`.

    :param args: see :attr:`args`

    :param kwargs: see :attr:`kwargs`.

    .. attribute:: task_name

        Kind of task. Must be a name registered in the task registry.

    .. attribute:: task_id

        UUID of the task.

    .. attribute:: args

        List of positional arguments to apply to the task.

    .. attribute:: kwargs

        Mapping of keyword arguments to apply to the task.

    .. attribute:: message

        The original message sent. Used for acknowledging the message.

    .. attribute executed

    Set if the task has been executed. A task should only be executed
    once.

    """
    success_msg = "Task %(name)s[%(id)s] processed: %(return_value)s"
    fail_msg = """
        Task %(name)s[%(id)s] raised exception: %(exc)s\n%(traceback)s
    """
    fail_email_subject = """
        [celery@%(hostname)s] Error: Task %(name)s (%(id)s): %(exc)s
    """
    fail_email_body = TASK_FAIL_EMAIL_BODY

    def __init__(self, task_name, task_id, args, kwargs,
            on_ack=noop, retries=0, **opts):
        self.task_name = task_name
        self.task_id = task_id
        self.retries = retries
        self.args = args
        self.kwargs = kwargs
        self.logger = kwargs.get("logger")
        self.on_ack = on_ack
        self.executed = False
        for opt in ("success_msg", "fail_msg", "fail_email_subject",
                "fail_email_body"):
            setattr(self, opt, opts.get(opt, getattr(self, opt, None)))
        if not self.logger:
            self.logger = get_default_logger()
        if self.task_name not in tasks:
            raise NotRegistered(self.task_name)
        self.task = tasks[self.task_name]

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
        retries = message_data.get("retries", 0)

        # Convert any unicode keys in the keyword arguments to ascii.
        kwargs = dict((key.encode("utf-8"), value)
                        for key, value in kwargs.items())

        return cls(task_name, task_id, args, kwargs,
                    retries=retries, on_ack=message.ack, logger=logger)

    def extend_with_default_kwargs(self, loglevel, logfile):
        """Extend the tasks keyword arguments with standard task arguments.

        Currently these are ``logfile``, ``loglevel``, ``task_id``,
        ``task_name`` and ``task_retries``.

        See :meth:`celery.task.base.Task.run` for more information.

        """
        kwargs = dict(self.kwargs)
        default_kwargs = {"logfile": logfile,
                            "loglevel": loglevel,
                            "task_id": self.task_id,
                            "task_name": self.task_name,
                            "task_retries": self.retries}
        fun = self.task.run
        supported_keys = fun_takes_kwargs(fun, default_kwargs)
        extend_with = dict((key, val) for key, val in default_kwargs.items()
                                if key in supported_keys)
        kwargs.update(extend_with)
        return kwargs

    def _get_tracer_args(self, loglevel=None, logfile=None):
        """Get the :class:`WorkerTaskTrace` tracer for this task."""
        task_func_kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        return self.task_name, self.task_id, self.args, task_func_kwargs

    def _set_executed_bit(self):
        """Set task as executed to make sure it's not executed again."""
        if self.executed:
            raise AlreadyExecutedError(
                   "Task %s[%s] has already been executed" % (
                       self.task_name, self.task_id))
        self.executed = True

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :class:`WorkerTaskTrace`.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        """
        # Make sure task has not already been executed.
        self._set_executed_bit()

        # acknowledge task as being processed.
        self.on_ack()

        tracer = WorkerTaskTrace(*self._get_tracer_args(loglevel, logfile))
        return tracer.execute()

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using the :mod:`multiprocessing` pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        :returns :class:`multiprocessing.AsyncResult` instance.

        """
        # Make sure task has not already been executed.
        self._set_executed_bit()

        args = self._get_tracer_args(loglevel, logfile)
        return pool.apply_async(execute_and_trace, args=args,
                callbacks=[self.on_success], errbacks=[self.on_failure],
                on_ack=self.on_ack)

    def on_success(self, ret_value):
        """The handler used if the task was successfully processed (
        without raising an exception)."""
        msg = self.success_msg.strip() % {
                "id": self.task_id,
                "name": self.task_name,
                "return_value": ret_value}
        self.logger.info(msg)

    def on_failure(self, exc_info):
        """The handler used if the task raised an exception."""
        from celery.conf import SEND_CELERY_TASK_ERROR_EMAILS

        context = {
            "hostname": socket.gethostname(),
            "id": self.task_id,
            "name": self.task_name,
            "exc": exc_info.exception,
            "traceback": exc_info.traceback,
            "args": self.args,
            "kwargs": self.kwargs,
        }
        self.logger.error(self.fail_msg.strip() % context)

        task_obj = tasks.get(self.task_name, object)
        send_error_email = SEND_CELERY_TASK_ERROR_EMAILS and not \
                getattr(task_obj, "disable_error_emails", False)
        if send_error_email:
            subject = self.fail_email_subject.strip() % context
            body = self.fail_email_body.strip() % context
            mail_admins(subject, body, fail_silently=True)

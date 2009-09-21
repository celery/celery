"""

Jobs Executable by the Worker Server.

"""
from celery.registry import tasks
from celery.exceptions import NotRegistered
from celery.execute import ExecuteWrapper
from celery.utils import noop, fun_takes_kwargs
from django.core.mail import mail_admins
import multiprocessing
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
Just to let you know,
celeryd at %%(hostname)s.
""" % {"EMAIL_SIGNATURE_SEP": EMAIL_SIGNATURE_SEP}


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
            on_ack=noop, retries=0, **opts):
        self.task_name = task_name
        self.task_id = task_id
        self.task_func = task_func
        self.retries = retries
        self.args = args
        self.kwargs = kwargs
        self.logger = kwargs.get("logger")
        self.on_ack = on_ack
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
        retries = message_data.get("retries", 0)

        # Convert any unicode keys in the keyword arguments to ascii.
        kwargs = dict((key.encode("utf-8"), value)
                        for key, value in kwargs.items())

        if task_name not in tasks:
            raise NotRegistered(task_name)
        task_func = tasks[task_name]
        return cls(task_name, task_id, task_func, args, kwargs,
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
        fun = getattr(self.task_func, "run", self.task_func)
        supported_keys = fun_takes_kwargs(fun, default_kwargs)
        extend_with = dict((key, val) for key, val in default_kwargs.items()
                                if key in supported_keys)
        kwargs.update(extend_with)
        return kwargs

    def _executeable(self, loglevel=None, logfile=None):
        """Get the :class:`celery.execute.ExecuteWrapper` for this task."""
        task_func_kwargs = self.extend_with_default_kwargs(loglevel, logfile)
        return ExecuteWrapper(self.task_func, self.task_id, self.task_name,
                              self.args, task_func_kwargs)

    def execute(self, loglevel=None, logfile=None):
        """Execute the task in a :class:`celery.execute.ExecuteWrapper`.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        """
        # acknowledge task as being processed.
        self.on_ack()

        return self._executeable(loglevel, logfile)()

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using the :mod:`multiprocessing` pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        :returns :class:`multiprocessing.AsyncResult` instance.

        """
        wrapper = self._executeable(loglevel, logfile)
        return pool.apply_async(wrapper,
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

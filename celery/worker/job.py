import os
import sys
import time
import socket
import warnings

from datetime import datetime

from celery import platforms
from celery.app import app_or_default
from celery.datastructures import ExceptionInfo
from celery.exceptions import SoftTimeLimitExceeded, TimeLimitExceeded
from celery.exceptions import WorkerLostError
from celery.execute.trace import TaskTrace
from celery.registry import tasks
from celery.utils import noop, kwdict, fun_takes_kwargs
from celery.utils import truncate_text
from celery.utils.timeutils import maybe_iso8601
from celery.worker import state

# pep8.py borks on a inline signature separator and
# says "trailing whitespace" ;)
EMAIL_SIGNATURE_SEP = "-- "
TASK_ERROR_EMAIL_BODY = """
Task %%(name)s with id %%(id)s raised exception:\n%%(exc)s


Task was called with args: %%(args)s kwargs: %%(kwargs)s.

The contents of the full traceback was:

%%(traceback)s

%(EMAIL_SIGNATURE_SEP)s
Just to let you know,
celeryd at %%(hostname)s.
""" % {"EMAIL_SIGNATURE_SEP": EMAIL_SIGNATURE_SEP}


WANTED_DELIVERY_INFO = ("exchange", "routing_key", "consumer_tag", )


class InvalidTaskError(Exception):
    """The task has invalid data or is not properly constructed."""


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

    :returns: the evaluated functions return value on success, or
        the exception instance on failure.

    """

    def __init__(self, *args, **kwargs):
        self.loader = kwargs.get("loader") or app_or_default().loader
        self.hostname = kwargs.get("hostname") or socket.gethostname()
        super(WorkerTaskTrace, self).__init__(*args, **kwargs)

        self._store_errors = True
        if self.task.ignore_result:
            self._store_errors = self.task.store_errors_even_if_ignored
        self.super = super(WorkerTaskTrace, self)

    def execute_safe(self, *args, **kwargs):
        """Same as :meth:`execute`, but catches errors."""
        try:
            return self.execute(*args, **kwargs)
        except Exception, exc:
            _type, _value, _tb = sys.exc_info()
            _value = self.task.backend.prepare_exception(exc)
            exc_info = ExceptionInfo((_type, _value, _tb))
            warnings.warn("Exception outside body: %s: %s\n%s" % tuple(
                map(str, (exc.__class__, exc, exc_info.traceback))))
            return exc_info

    def execute(self):
        """Execute, trace and store the result of the task."""
        self.loader.on_task_init(self.task_id, self.task)
        if self.task.track_started:
            self.task.backend.mark_as_started(self.task_id,
                                              pid=os.getpid(),
                                              hostname=self.hostname)
        try:
            return super(WorkerTaskTrace, self).execute()
        finally:
            self.task.backend.process_cleanup()
            self.loader.on_process_cleanup()

    def handle_success(self, retval, *args):
        """Handle successful execution."""
        if not self.task.ignore_result:
            self.task.backend.mark_as_done(self.task_id, retval)
        return self.super.handle_success(retval, *args)

    def handle_retry(self, exc, type_, tb, strtb):
        """Handle retry exception."""
        message, orig_exc = exc.args
        if self._store_errors:
            self.task.backend.mark_as_retry(self.task_id, orig_exc, strtb)
        self.super.handle_retry(exc, type_, tb, strtb)

    def handle_failure(self, exc, type_, tb, strtb):
        """Handle exception."""
        if self._store_errors:
            exc = self.task.backend.mark_as_failure(self.task_id, exc, strtb)
        else:
            exc = self.task.backend.prepare_exception(exc)
        return self.super.handle_failure(exc, type_, tb, strtb)


def execute_and_trace(task_name, *args, **kwargs):
    """This is a pickleable method used as a target when applying to pools.

    It's the same as::

        >>> WorkerTaskTrace(task_name, *args, **kwargs).execute_safe()

    """
    hostname = kwargs.get("hostname")
    platforms.set_mp_process_title("celeryd", info=task_name,
                                   hostname=hostname)
    try:
        return WorkerTaskTrace(task_name, *args, **kwargs).execute_safe()
    finally:
        platforms.set_mp_process_title("celeryd", hostname=hostname)


class TaskRequest(object):
    """A request for task execution.

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

    .. attribute:: on_ack

        Callback called when the task should be acknowledged.

    .. attribute:: message

        The original message sent. Used for acknowledging the message.

    .. attribute:: executed

        Set to :const:`True` if the task has been executed.
        A task should only be executed once.

    .. attribute:: delivery_info

        Additional delivery info, e.g. the contains the path
        from producer to consumer.

    .. attribute:: acknowledged

        Set to :const:`True` if the task has been acknowledged.

    """
    # Logging output
    success_msg = "Task %(name)s[%(id)s] processed: %(return_value)s"
    error_msg = """
        Task %(name)s[%(id)s] raised exception: %(exc)s\n%(traceback)s
    """

    # E-mails
    email_subject = """
        [celery@%(hostname)s] Error: Task %(name)s (%(id)s): %(exc)s
    """
    email_body = TASK_ERROR_EMAIL_BODY

    # Internal flags
    executed = False
    acknowledged = False
    time_start = None
    _already_revoked = False

    def __init__(self, task_name, task_id, args, kwargs,
            on_ack=noop, retries=0, delivery_info=None, hostname=None,
            email_subject=None, email_body=None, logger=None,
            eventer=None, eta=None, expires=None, app=None, **opts):
        self.app = app_or_default(app)
        self.task_name = task_name
        self.task_id = task_id
        self.retries = retries
        self.args = args
        self.kwargs = kwargs
        self.eta = eta
        self.expires = expires
        self.on_ack = on_ack
        self.delivery_info = delivery_info or {}
        self.hostname = hostname or socket.gethostname()
        self.logger = logger or self.app.log.get_default_logger()
        self.eventer = eventer
        self.email_subject = email_subject or self.email_subject
        self.email_body = email_body or self.email_body

        self.task = tasks[self.task_name]
        self._store_errors = True
        if self.task.ignore_result:
            self._store_errors = self.task.store_errors_even_if_ignored

    def maybe_expire(self):
        if self.expires and datetime.now() > self.expires:
            state.revoked.add(self.task_id)
            if self._store_errors:
                self.task.backend.mark_as_revoked(self.task_id)

    def revoked(self):
        if self._already_revoked:
            return True
        if self.expires:
            self.maybe_expire()
        if self.task_id in state.revoked:
            self.logger.warn("Skipping revoked task: %s[%s]" % (
                self.task_name, self.task_id))
            self.send_event("task-revoked", uuid=self.task_id)
            self.acknowledge()
            self._already_revoked = True
            return True
        return False

    @classmethod
    def from_message(cls, message, message_data, logger=None, eventer=None,
            hostname=None, app=None):
        """Create a :class:`TaskRequest` from a task message sent by
        :class:`celery.app.amqp.TaskPublisher`.

        :raises UnknownTaskError: if the message does not describe a task,
            the message is also rejected.

        :returns :class:`TaskRequest`:

        """
        task_name = message_data["task"]
        task_id = message_data["id"]
        args = message_data["args"]
        kwargs = message_data["kwargs"]
        retries = message_data.get("retries", 0)
        eta = maybe_iso8601(message_data.get("eta"))
        expires = maybe_iso8601(message_data.get("expires"))

        _delivery_info = getattr(message, "delivery_info", {})
        delivery_info = dict((key, _delivery_info.get(key))
                                for key in WANTED_DELIVERY_INFO)

        if not hasattr(kwargs, "items"):
            raise InvalidTaskError("Task kwargs must be a dictionary.")

        return cls(task_name, task_id, args, kwdict(kwargs),
                   retries=retries, on_ack=message.ack,
                   delivery_info=delivery_info, logger=logger,
                   eventer=eventer, hostname=hostname,
                   eta=eta, expires=expires, app=app)

    def extend_with_default_kwargs(self, loglevel, logfile):
        """Extend the tasks keyword arguments with standard task arguments.

        Currently these are ``logfile``, ``loglevel``, ``task_id``,
        ``task_name``, ``task_retries``, and ``delivery_info``.

        See :meth:`celery.task.base.Task.run` for more information.

        """
        kwargs = dict(self.kwargs)
        default_kwargs = {"logfile": logfile,
                          "loglevel": loglevel,
                          "task_id": self.task_id,
                          "task_name": self.task_name,
                          "task_retries": self.retries,
                          "task_is_eager": False,
                          "delivery_info": self.delivery_info}
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
        if self.revoked():
            return
        # Make sure task has not already been executed.
        self._set_executed_bit()

        # acknowledge task as being processed.
        if not self.task.acks_late:
            self.acknowledge()

        tracer = WorkerTaskTrace(*self._get_tracer_args(loglevel, logfile),
                                 **{"hostname": self.hostname,
                                    "loader": self.app.loader})
        retval = tracer.execute()
        self.acknowledge()
        return retval

    def send_event(self, type, **fields):
        if self.eventer:
            self.eventer.send(type, **fields)

    def execute_using_pool(self, pool, loglevel=None, logfile=None):
        """Like :meth:`execute`, but using the :mod:`multiprocessing` pool.

        :param pool: A :class:`multiprocessing.Pool` instance.

        :keyword loglevel: The loglevel used by the task.

        :keyword logfile: The logfile used by the task.

        """
        if self.revoked():
            return
        # Make sure task has not already been executed.
        self._set_executed_bit()

        args = self._get_tracer_args(loglevel, logfile)
        self.time_start = time.time()
        result = pool.apply_async(execute_and_trace,
                                  args=args,
                                  kwargs={"hostname": self.hostname},
                                  accept_callback=self.on_accepted,
                                  timeout_callback=self.on_timeout,
                                  callbacks=[self.on_success],
                                  errbacks=[self.on_failure])
        return result

    def on_accepted(self):
        state.task_accepted(self)
        if not self.task.acks_late:
            self.acknowledge()
        self.send_event("task-started", uuid=self.task_id)
        self.logger.debug("Task accepted: %s[%s]" % (
            self.task_name, self.task_id))

    def on_timeout(self, soft):
        state.task_ready(self)
        if soft:
            self.logger.warning("Soft time limit exceeded for %s[%s]" % (
                self.task_name, self.task_id))
            exc = SoftTimeLimitExceeded()
        else:
            self.logger.error("Hard time limit exceeded for %s[%s]" % (
                self.task_name, self.task_id))
            exc = TimeLimitExceeded()

        self.task.backend.mark_as_failure(self.task_id, exc)

    def acknowledge(self):
        if not self.acknowledged:
            self.on_ack()
            self.acknowledged = True

    def on_success(self, ret_value):
        """The handler used if the task was successfully processed (
        without raising an exception)."""
        state.task_ready(self)

        if self.task.acks_late:
            self.acknowledge()

        runtime = time.time() - self.time_start
        self.send_event("task-succeeded", uuid=self.task_id,
                        result=repr(ret_value), runtime=runtime)

        msg = self.success_msg.strip() % {
                "id": self.task_id,
                "name": self.task_name,
                "return_value": self.repr_result(ret_value)}
        self.logger.info(msg)

    def repr_result(self, result, maxlen=46):
        # 46 is the length needed to fit
        #     "the quick brown fox jumps over the lazy dog" :)
        return truncate_text(repr(result), maxlen)

    def on_failure(self, exc_info):
        """The handler used if the task raised an exception."""
        state.task_ready(self)

        if self.task.acks_late:
            self.acknowledge()

        self.send_event("task-failed", uuid=self.task_id,
                                       exception=repr(exc_info.exception),
                                       traceback=exc_info.traceback)

        # This is a special case as the process would not have had
        # time to write the result.
        if isinstance(exc_info.exception, WorkerLostError):
            self.task.backend.mark_as_failure(self.task_id,
                                              exc_info.exception)

        context = {"hostname": self.hostname,
                   "id": self.task_id,
                   "name": self.task_name,
                   "exc": repr(exc_info.exception),
                   "traceback": unicode(exc_info.traceback, 'utf-8'),
                   "args": self.args,
                   "kwargs": self.kwargs}
        self.logger.error(self.error_msg.strip() % context)

        task_obj = tasks.get(self.task_name, object)
        self.send_error_email(task_obj, context, exc_info.exception,
                              enabled=task_obj.send_error_emails,
                              whitelist=task_obj.error_whitelist)

    def send_error_email(self, task, context, exc,
            whitelist=None, enabled=False, fail_silently=True):
        if enabled and not task.disable_error_emails:
            if whitelist:
                if not isinstance(exc, tuple(whitelist)):
                    return
            subject = self.email_subject.strip() % context
            body = self.email_body.strip() % context
            self.app.mail_admins(subject, body, fail_silently=fail_silently)

    def __repr__(self):
        return '<%s: {name:"%s", id:"%s", args:"%s", kwargs:"%s"}>' % (
                self.__class__.__name__,
                self.task_name, self.task_id,
                self.args, self.kwargs)

    def info(self, safe=False):
        args = self.args
        kwargs = self.kwargs
        if not safe:
            args = repr(args)
            kwargs = repr(self.kwargs)

        return {"id": self.task_id,
                "name": self.task_name,
                "args": args,
                "kwargs": kwargs,
                "hostname": self.hostname,
                "time_start": self.time_start,
                "acknowledged": self.acknowledged,
                "delivery_info": self.delivery_info}

    def shortinfo(self):
        return "%s[%s]%s%s" % (
                    self.task_name,
                    self.task_id,
                    self.eta and " eta:[%s]" % (self.eta, ) or "",
                    self.expires and " expires:[%s]" % (self.expires, ) or "")

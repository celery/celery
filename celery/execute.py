import sys
import inspect
import traceback
from datetime import datetime, timedelta

from billiard.utils.functional import curry

from celery import conf
from celery import signals
from celery.utils import gen_unique_id, noop, fun_takes_kwargs
from celery.result import AsyncResult, EagerResult
from celery.registry import tasks
from celery.messaging import TaskPublisher, with_connection_inline
from celery.exceptions import RetryTaskError
from celery.datastructures import ExceptionInfo

TASK_EXEC_OPTIONS = ("routing_key", "exchange",
                     "immediate", "mandatory",
                     "priority", "serializer")


def apply_async(task, args=None, kwargs=None, countdown=None, eta=None,
        task_id=None, publisher=None, connection=None, connect_timeout=None,
        **options):
    """Run a task asynchronously by the celery daemon(s).

    :param task: The task to run (a callable object, or a :class:`Task`
        instance

    :keyword args: The positional arguments to pass on to the
        task (a ``list``).

    :keyword kwargs: The keyword arguments to pass on to the task (a ``dict``)

    :keyword countdown: Number of seconds into the future that the task should
        execute. Defaults to immediate delivery (Do not confuse that with
        the ``immediate`` setting, they are unrelated).

    :keyword eta: A :class:`datetime.datetime` object that describes the
        absolute time when the task should execute. May not be specified
        if ``countdown`` is also supplied. (Do not confuse this with the
        ``immediate`` setting, they are unrelated).

    :keyword routing_key: The routing key used to route the task to a worker
        server.

    :keyword exchange: The named exchange to send the task to. Defaults to
        :attr:`celery.task.base.Task.exchange`.

    :keyword immediate: Request immediate delivery. Will raise an exception
        if the task cannot be routed to a worker immediately.
        (Do not confuse this parameter with the ``countdown`` and ``eta``
        settings, as they are unrelated).

    :keyword mandatory: Mandatory routing. Raises an exception if there's
        no running workers able to take on this task.

    :keyword connection: Re-use existing AMQP connection.
        The ``connect_timeout`` argument is not respected if this is set.

    :keyword connect_timeout: The timeout in seconds, before we give up
        on establishing a connection to the AMQP server.

    :keyword priority: The task priority, a number between ``0`` and ``9``.

    :keyword serializer: A string identifying the default serialization
        method to use. Defaults to the ``CELERY_TASK_SERIALIZER`` setting.
        Can be ``pickle`` ``json``, ``yaml``, or any custom serialization
        methods that have been registered with
        :mod:`carrot.serialization.registry`.

    **Note**: If the ``CELERY_ALWAYS_EAGER`` setting is set, it will be
    replaced by a local :func:`apply` call instead.

    """
    if conf.ALWAYS_EAGER:
        return apply(task, args, kwargs)

    for option_name in TASK_EXEC_OPTIONS:
        if option_name not in options:
            options[option_name] = getattr(task, option_name, None)

    if countdown: # Convert countdown to ETA.
        eta = datetime.now() + timedelta(seconds=countdown)

    def _delay_task(connection):
        publish = publisher or TaskPublisher(connection)
        try:
            return publish.delay_task(task.name, args or [], kwargs or {},
                                      task_id=task_id,
                                      eta=eta,
                                      **options)
        finally:
            publisher or publish.close()

    task_id = with_connection_inline(_delay_task, connection=connection,
                                     connect_timeout=connect_timeout)
    return AsyncResult(task_id)


def delay_task(task_name, *args, **kwargs):
    """Delay a task for execution by the ``celery`` daemon.

    :param task_name: the name of a task registered in the task registry.

    :param \*args: positional arguments to pass on to the task.

    :param \*\*kwargs: keyword arguments to pass on to the task.

    :raises celery.exceptions.NotRegistered: exception if no such task
        has been registered in the task registry.

    :rtype: :class:`celery.result.AsyncResult`.

    Example

        >>> r = delay_task("update_record", name="George Constanza", age=32)
        >>> r.ready()
        True
        >>> r.result
        "Record was updated"

    """
    if task_name not in tasks:
        raise tasks.NotRegistered(
                "Task with name %s not registered in the task registry." % (
                    task_name))
    task = tasks[task_name]
    return apply_async(task, args, kwargs)


def apply(task, args, kwargs, **options):
    """Apply the task locally.

    This will block until the task completes, and returns a
    :class:`celery.result.EagerResult` instance.

    """
    args = args or []
    kwargs = kwargs or {}
    task_id = gen_unique_id()
    retries = options.get("retries", 0)

    # If it's a Task class we need to instantiate it, so it's callable.
    task = inspect.isclass(task) and task() or task

    default_kwargs = {"task_name": task.name,
                      "task_id": task_id,
                      "task_retries": retries,
                      "task_is_eager": True,
                      "logfile": None,
                      "loglevel": 0}
    supported_keys = fun_takes_kwargs(task.run, default_kwargs)
    extend_with = dict((key, val) for key, val in default_kwargs.items()
                            if key in supported_keys)
    kwargs.update(extend_with)

    trace = TaskTrace(task.name, task_id, args, kwargs, task=task)
    retval = trace.execute()

    return EagerResult(task_id, retval, trace.status,
                       traceback=trace.strtb)


class TraceInfo(object):
    def __init__(self, status="PENDING", retval=None, exc_info=None):
        self.status = status
        self.retval = retval
        self.exc_info = exc_info
        self.exc_type = None
        self.exc_value = None
        self.tb = None
        self.strtb = None
        if self.exc_info:
            self.exc_type, self.exc_value, self.tb = exc_info
            self.strtb = "\n".join(traceback.format_exception(*exc_info))

    @classmethod
    def trace(cls, fun, args, kwargs):
        """Trace the execution of a function, calling the appropiate callback
        if the function raises retry, an failure or returned successfully."""
        try:
            return cls("SUCCESS", retval=fun(*args, **kwargs))
        except (SystemExit, KeyboardInterrupt):
            raise
        except RetryTaskError, exc:
            return cls("RETRY", retval=exc, exc_info=sys.exc_info())
        except Exception, exc:
            return cls("FAILURE", retval=exc, exc_info=sys.exc_info())


class TaskTrace(object):

    def __init__(self, task_name, task_id, args, kwargs, task=None):
        self.task_id = task_id
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs
        self.task = task or tasks[self.task_name]
        self.status = "PENDING"
        self.strtb = None
        self._trace_handlers = {"FAILURE": self.handle_failure,
                                "RETRY": self.handle_retry,
                                "SUCCESS": self.handle_success}

    def __call__(self):
        return self.execute()

    def execute(self):
        signals.task_prerun.send(sender=self.task, task_id=self.task_id,
                                 task=self.task, args=self.args,
                                 kwargs=self.kwargs)
        retval = self._trace()

        signals.task_postrun.send(sender=self.task, task_id=self.task_id,
                                  task=self.task, args=self.args,
                                  kwargs=self.kwargs, retval=retval)
        return retval

    def _trace(self):
        trace = TraceInfo.trace(self.task, self.args, self.kwargs)
        self.status = trace.status
        self.strtb = trace.strtb
        handler = self._trace_handlers[trace.status]
        return handler(trace.retval, trace.exc_type, trace.tb, trace.strtb)

    def handle_success(self, retval, *args):
        """Handle successful execution."""
        self.task.on_success(retval, self.task_id, self.args, self.kwargs)
        return retval

    def handle_retry(self, exc, type_, tb, strtb):
        """Handle retry exception."""
        self.task.on_retry(exc, self.task_id, self.args, self.kwargs)

        # Create a simpler version of the RetryTaskError that stringifies
        # the original exception instead of including the exception instance.
        # This is for reporting the retry in logs, e-mail etc, while
        # guaranteeing pickleability.
        message, orig_exc = exc.args
        expanded_msg = "%s: %s" % (message, str(orig_exc))
        return ExceptionInfo((type_,
                              type_(expanded_msg, None),
                              tb))

    def handle_failure(self, exc, type_, tb, strtb):
        """Handle exception."""
        self.task.on_failure(exc, self.task_id, self.args, self.kwargs)
        return ExceptionInfo((type_, exc, tb))

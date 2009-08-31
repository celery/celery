from carrot.connection import DjangoBrokerConnection
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.result import AsyncResult, EagerResult
from celery.messaging import TaskPublisher
from celery.registry import tasks
from celery.utils import gen_unique_id, noop, fun_takes_kwargs
from functools import partial as curry
from datetime import datetime, timedelta
from multiprocessing import get_logger
from celery.exceptions import RetryTaskError
from celery.datastructures import ExceptionInfo
from celery.backends import default_backend
from celery.loaders import current_loader
from celery.monitoring import TaskTimerStats
from celery import signals
import sys
import traceback
import inspect


def apply_async(task, args=None, kwargs=None, countdown=None, eta=None,
        routing_key=None, exchange=None, task_id=None,
        immediate=None, mandatory=None, priority=None, connection=None,
        connect_timeout=AMQP_CONNECTION_TIMEOUT, serializer=None, **opts):
    """Run a task asynchronously by the celery daemon(s).

    :param task: The task to run (a callable object, or a :class:`Task`
        instance

    :param args: The positional arguments to pass on to the task (a ``list``).

    :param kwargs: The keyword arguments to pass on to the task (a ``dict``)

    :param countdown: Number of seconds into the future that the task should
        execute. Defaults to immediate delivery (Do not confuse that with
        the ``immediate`` setting, they are unrelated).

    :param eta: A :class:`datetime.datetime` object that describes the
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

    """
    args = args or []
    kwargs = kwargs or {}
    routing_key = routing_key or getattr(task, "routing_key", None)
    exchange = exchange or getattr(task, "exchange", None)
    immediate = immediate or getattr(task, "immediate", None)
    mandatory = mandatory or getattr(task, "mandatory", None)
    priority = priority or getattr(task, "priority", None)
    serializer = serializer or getattr(task, "serializer", None)
    taskset_id = opts.get("taskset_id")
    publisher = opts.get("publisher")
    retries = opts.get("retries")
    if countdown:
        eta = datetime.now() + timedelta(seconds=countdown)

    from celery.conf import ALWAYS_EAGER
    if ALWAYS_EAGER:
        return apply(task, args, kwargs)

    need_to_close_connection = False
    if not publisher:
        if not connection:
            connection = DjangoBrokerConnection(
                            connect_timeout=connect_timeout)
            need_to_close_connection = True
        publisher = TaskPublisher(connection=connection)

    delay_task = publisher.delay_task
    if taskset_id:
        delay_task = curry(publisher.delay_task_in_set, taskset_id)

    task_id = delay_task(task.name, args, kwargs,
                         task_id=task_id, retries=retries,
                         routing_key=routing_key, exchange=exchange,
                         mandatory=mandatory, immediate=immediate,
                         serializer=serializer, priority=priority,
                         eta=eta)

    if need_to_close_connection:
        publisher.close()
        connection.close()

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

    # If it's a Task class we need to have to instance
    # for it to be callable.
    task = inspect.isclass(task) and task() or task

    default_kwargs = {"task_name": task.name,
                      "task_id": task_id,
                      "task_retries": retries,
                      "task_is_eager": True,
                      "logfile": None,
                      "loglevel": 0}
    fun = getattr(task, "run", task)
    supported_keys = fun_takes_kwargs(fun, default_kwargs)
    extend_with = dict((key, val) for key, val in default_kwargs.items()
                            if key in supported_keys)
    kwargs.update(extend_with)

    try:
        ret_value = task(*args, **kwargs)
        status = "DONE"
        strtb = None
    except Exception, exc:
        type_, value_, tb = sys.exc_info()
        strtb = "\n".join(traceback.format_exception(type_, value_, tb))
        ret_value = exc
        status = "FAILURE"

    return EagerResult(task_id, ret_value, status, traceback=strtb)


class ExecuteWrapper(object):
    """Wraps the task in a jail, which catches all exceptions, and
    saves the status and result of the task execution to the task
    meta backend.

    If the call was successful, it saves the result to the task result
    backend, and sets the task status to ``"DONE"``.

    If the call raises :exc:`celery.exceptions.RetryTaskError`, it extracts
    the original exception, uses that as the result and sets the task status
    to ``"RETRY"``.

    If the call results in an exception, it saves the exception as the task
    result, and sets the task status to ``"FAILURE"``.

    :param fun: Callable object to execute.
    :param task_id: The unique id of the task.
    :param task_name: Name of the task.
    :param args: List of positional args to pass on to the function.
    :param kwargs: Keyword arguments mapping to pass on to the function.

    :returns: the function return value on success, or
        the exception instance on failure.

    """

    def __init__(self, fun, task_id, task_name, args=None, kwargs=None):
        self.fun = fun
        self.task_id = task_id
        self.task_name = task_name
        self.args = args or []
        self.kwargs = kwargs or {}

    def __call__(self, *args, **kwargs):
        return self.execute()

    def execute(self):
        # Convenience variables
        fun = self.fun
        task_id = self.task_id
        task_name = self.task_name
        args = self.args
        kwargs = self.kwargs

        # Run task loader init handler.
        current_loader.on_task_init(task_id, fun)

        # Backend process cleanup
        default_backend.process_cleanup()

        # Send pre-run signal.
        signals.task_prerun.send(sender=fun, task_id=task_id, task=fun,
                                 args=args, kwargs=kwargs)

        retval = None
        timer_stat = TaskTimerStats.start(task_id, task_name, args, kwargs)
        try:
            result = fun(*args, **kwargs)
        except (SystemExit, KeyboardInterrupt):
            raise
        except RetryTaskError, exc:
            retval = self.handle_retry(exc, sys.exc_info())
        except Exception, exc:
            retval = self.handle_failure(exc, sys.exc_info())
        else:
            retval = self.handle_success(result)
        finally:
            timer_stat.stop()

        # Send post-run signal.
        signals.task_postrun.send(sender=fun, task_id=task_id, task=fun,
                                  args=args, kwargs=kwargs, retval=retval)

        return retval

    def handle_success(self, retval):
        """Handle successful execution.

        Saves the result to the current result store (skipped if the callable
            has a ``ignore_result`` attribute set to ``True``).

        If the callable has a ``on_success`` function, it as called with
        ``retval`` as argument.

        :param retval: The return value.

        """
        if not getattr(self.fun, "ignore_result", False):
            default_backend.mark_as_done(self.task_id, retval)

        # Run success handler last to be sure the status is saved.
        success_handler = getattr(self.fun, "on_success", noop)
        success_handler(retval, self.task_id, self.args, self.kwargs)

        return retval

    def handle_retry(self, exc, exc_info):
        """Handle retry exception."""
        ### Task is to be retried.
        type_, value_, tb = exc_info
        strtb = "\n".join(traceback.format_exception(type_, value_, tb))

        # RetryTaskError stores both a small message describing the retry
        # and the original exception.
        message, orig_exc = exc.args
        default_backend.mark_as_retry(self.task_id, orig_exc, strtb)

        # Create a simpler version of the RetryTaskError that stringifies
        # the original exception instead of including the exception instance.
        # This is for reporting the retry in logs, e-mail etc, while
        # guaranteeing pickleability.
        expanded_msg = "%s: %s" % (message, str(orig_exc))
        retval = ExceptionInfo((type_,
                                type_(expanded_msg, None),
                                tb))

        # Run retry handler last to be sure the status is saved.
        retry_handler = getattr(self.fun, "on_retry", noop)
        retry_handler(exc, self.task_id, self.args, self.kwargs)

        return retval

    def handle_failure(self, exc, exc_info):
        """Handle exception."""
        ### Task ended in failure.
        type_, value_, tb = exc_info
        strtb = "\n".join(traceback.format_exception(type_, value_, tb))

        # mark_as_failure returns an exception that is guaranteed to
        # be pickleable.
        stored_exc = default_backend.mark_as_failure(self.task_id, exc, strtb)

        # wrap exception info + traceback and return it to caller.
        retval = ExceptionInfo((type_, stored_exc, tb))

        # Run error handler last to be sure the status is stored.
        error_handler = getattr(self.fun, "on_failure", noop)
        error_handler(stored_exc, self.task_id, self.args, self.kwargs)

        return retval

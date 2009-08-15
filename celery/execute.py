from carrot.connection import DjangoBrokerConnection
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.result import AsyncResult, EagerResult
from celery.messaging import TaskPublisher
from celery.registry import tasks
from celery.utils import gen_unique_id
from functools import partial as curry
from datetime import datetime, timedelta
from multiprocessing import get_logger
import inspect


def apply_async(task, args=None, kwargs=None, countdown=None, eta=None,
        routing_key=None, exchange=None, task_id=None,
        immediate=None, mandatory=None, priority=None, connection=None,
        connect_timeout=AMQP_CONNECTION_TIMEOUT, **opts):
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

    """
    args = args or []
    kwargs = kwargs or {}
    routing_key = routing_key or getattr(task, "routing_key", None)
    exchange = exchange or getattr(task, "exchange", None)
    immediate = immediate or getattr(task, "immediate", None)
    mandatory = mandatory or getattr(task, "mandatory", None)
    priority = priority or getattr(task, "priority", None)
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
                         priority=priority, eta=eta)

    if need_to_close_connection:
        publisher.close()
        connection.close()

    return AsyncResult(task_id)


def delay_task(task_name, *args, **kwargs):
    """Delay a task for execution by the ``celery`` daemon.

    :param task_name: the name of a task registered in the task registry.

    :param \*args: positional arguments to pass on to the task.

    :param \*\*kwargs: keyword arguments to pass on to the task.

    :raises celery.registry.NotRegistered: exception if no such task
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

    kwargs.update({"task_name": task.name,
                   "task_id": task_id,
                   "task_retries": retries,
                   "task_is_eager": True,
                   "logfile": None,
                   "loglevel": 0})

    try:
        ret_value = task(*args, **kwargs)
        status = "DONE"
    except Exception, exc:
        ret_value = exc
        status = "FAILURE"

    return EagerResult(task_id, ret_value, status)

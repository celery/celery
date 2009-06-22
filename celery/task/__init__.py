"""

Working with tasks and task sets.

"""
from carrot.connection import DjangoAMQPConnection
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.messaging import TaskPublisher
from celery.registry import tasks
from celery.backends import default_backend
from celery.result import AsyncResult
from celery.task.base import Task, TaskSet, PeriodicTask
from celery.task.builtins import AsynchronousMapTask, ExecuteRemoteTask
from celery.task.builtins import DeleteExpiredTaskMetaTask, PingTask
from functools import partial as curry
try:
    import cPickle as pickle
except ImportError:
    import pickle


def apply_async(task, args=None, kwargs=None, routing_key=None,
        immediate=None, mandatory=None, connection=None,
        connect_timeout=AMQP_CONNECTION_TIMEOUT, priority=None, **opts):
    """Run a task asynchronously by the celery daemon(s).

    :param task: The task to run (a callable object, or a :class:`Task`
        instance

    :param args: The positional arguments to pass on to the task (a ``list``).

    :param kwargs: The keyword arguments to pass on to the task (a ``dict``)


    :keyword routing_key: The routing key used to route the task to a worker
        server.

    :keyword immediate: Request immediate delivery. Will raise an exception
        if the task cannot be routed to a worker immediately.

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
    immediate = immediate or getattr(task, "immediate", None)
    mandatory = mandatory or getattr(task, "mandatory", None)
    priority = priority or getattr(task, "priority", None)
    taskset_id = opts.get("taskset_id")
    publisher = opts.get("publisher")

    need_to_close_connection = False
    if not publisher:
        if not connection:
            connection = DjangoAMQPConnection(connect_timeout=connect_timeout)
            need_to_close_connection = True
        publisher = TaskPublisher(connection=connection)

    delay_task = publisher.delay_task
    if taskset_id:
        delay_task = curry(publisher.delay_task_in_set, taskset_id)
        
    task_id = delay_task(task.name, args, kwargs,
                         routing_key=routing_key, mandatory=mandatory,
                         immediate=immediate, priority=priority)

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


def discard_all(connect_timeout=AMQP_CONNECTION_TIMEOUT):
    """Discard all waiting tasks.

    This will ignore all tasks waiting for execution, and they will
    be deleted from the messaging server.

    :returns: the number of tasks discarded.

    :rtype: int

    """
    amqp_connection = DjangoAMQPConnection(connect_timeout=connect_timeout)
    consumer = TaskConsumer(connection=amqp_connection)
    discarded_count = consumer.discard_all()
    amqp_connection.close()
    return discarded_count


def is_done(task_id):
    """Returns ``True`` if task with ``task_id`` has been executed.

    :rtype: bool

    """
    return default_backend.is_done(task_id)


def dmap(func, args, timeout=None):
    """Distribute processing of the arguments and collect the results.

    Example

        >>> from celery.task import map
        >>> import operator
        >>> dmap(operator.add, [[2, 2], [4, 4], [8, 8]])
        [4, 8, 16]

    """
    return TaskSet.map(func, args, timeout=timeout)


def dmap_async(func, args, timeout=None):
    """Distribute processing of the arguments and collect the results
    asynchronously.

    :returns: :class:`celery.result.AsyncResult` object.

    Example

        >>> from celery.task import dmap_async
        >>> import operator
        >>> presult = dmap_async(operator.add, [[2, 2], [4, 4], [8, 8]])
        >>> presult
        <AsyncResult: 373550e8-b9a0-4666-bc61-ace01fa4f91d>
        >>> presult.status
        'DONE'
        >>> presult.result
        [4, 8, 16]

    """
    return TaskSet.map_async(func, args, timeout=timeout)


def execute_remote(func, *args, **kwargs):
    """Execute arbitrary function/object remotely.

    :param func: A callable function or object.

    :param \*args: Positional arguments to apply to the function.

    :param \*\*kwargs: Keyword arguments to apply to the function.

    The object must be picklable, so you can't use lambdas or functions
    defined in the REPL (the objects must have an associated module).

    :returns: class:`celery.result.AsyncResult`.

    """
    return ExecuteRemoteTask.delay(pickle.dumps(func), args, kwargs)


def ping():
    """Test if the server is alive.

    Example:

        >>> from celery.task import ping
        >>> ping()
        'pong'
    """
    return PingTask.apply_async().get()

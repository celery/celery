from carrot.connection import DjangoAMQPConnection
from celery.conf import AMQP_CONNECTION_TIMEOUT
from celery.result import AsyncResult
from celery.messaging import TaskPublisher
from functools import partial as curry
from datetime import datetime, timedelta


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
    if countdown:
        eta = datetime.now() + timedelta(seconds=countdown)

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
                         immediate=immediate, priority=priority,
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

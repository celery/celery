"""

Working with tasks and task sets.

"""
from billiard.serialization import pickle

from celery.execute import apply_async
from celery.registry import tasks
from celery.task.base import Task, TaskSet, PeriodicTask, ExecuteRemoteTask
from celery.task.control import discard_all
from celery.task.builtins import PingTask
from celery.task.http import HttpDispatchTask

__all__ = ["Task", "TaskSet", "PeriodicTask", "tasks", "discard_all",
           "dmap", "dmap_async", "execute_remote", "ping"]


def dmap(fun, args, timeout=None):
    """Distribute processing of the arguments and collect the results.

    Example

        >>> from celery.task import dmap
        >>> import operator
        >>> dmap(operator.add, [[2, 2], [4, 4], [8, 8]])
        [4, 8, 16]

    """
    return TaskSet.map(fun, args, timeout=timeout)


def dmap_async(fun, args, timeout=None):
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
        'SUCCESS'
        >>> presult.result
        [4, 8, 16]

    """
    return TaskSet.map_async(fun, args, timeout=timeout)


def execute_remote(fun, *args, **kwargs):
    """Execute arbitrary function/object remotely.

    :param fun: A callable function or object.
    :param \*args: Positional arguments to apply to the function.
    :param \*\*kwargs: Keyword arguments to apply to the function.

    The object must be picklable, so you can't use lambdas or functions
    defined in the REPL (the objects must have an associated module).

    :returns: class:`celery.result.AsyncResult`.

    """
    return ExecuteRemoteTask.delay(pickle.dumps(fun), args, kwargs)


def ping():
    """Test if the server is alive.

    Example:

        >>> from celery.task import ping
        >>> ping()
        'pong'
    """
    return PingTask.apply_async().get()

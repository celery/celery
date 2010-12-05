"""

Working with tasks and task sets.

"""
import warnings

from celery.task.base import Task, PeriodicTask
from celery.task.sets import TaskSet, subtask
from celery.task.builtins import PingTask
from celery.task.control import discard_all

__all__ = ["Task", "TaskSet", "PeriodicTask", "subtask", "discard_all"]


def ping():
    """Deprecated and scheduled for removal in Celery 2.3.

    Please use :meth:`celery.task.control.ping` instead.

    """
    warnings.warn(DeprecationWarning(
        "The ping task has been deprecated and will be removed in Celery "
        "v2.3.  Please use inspect.ping instead."))
    return PingTask.apply_async().get()

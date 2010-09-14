"""

Decorators

"""
from inspect import getargspec

from celery.app import default_app
from celery.task.base import PeriodicTask


task = default_app.task


def periodic_task(**options):
    """Task decorator to create a periodic task.

    Example task, scheduling a task once every day:

    .. code-block:: python

        from datetime import timedelta

        @periodic_task(run_every=timedelta(days=1))
        def cronjob(**kwargs):
            logger = cronjob.get_logger(**kwargs)
            logger.warn("Task running...")

    """
    return task(**dict({"base": PeriodicTask}, **options))

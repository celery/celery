"""

Decorators

"""
from celery.app import app_or_default
from celery.task.base import PeriodicTask


def task(*args, **kwargs):
    return app_or_default().task(*args, **kwargs)


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

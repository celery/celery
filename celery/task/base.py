# -*- coding: utf-8 -*-
"""
    celery.task.base
    ~~~~~~~~~~~~~~~~

    The task implementation has been moved to :mod:`celery.app.task`.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .. import current_app
from ..app.task import Context, TaskType, BaseTask  # noqa
from ..schedules import maybe_schedule
from ..utils import deprecated, timeutils

Task = current_app.Task


@deprecated("Importing TaskSet from celery.task.base",
            alternative="Use celery.task.TaskSet instead.",
            removal="2.4")
def TaskSet(*args, **kwargs):
    from celery.task.sets import TaskSet
    return TaskSet(*args, **kwargs)


@deprecated("Importing subtask from celery.task.base",
            alternative="Use celery.task.subtask instead.",
            removal="2.4")
def subtask(*args, **kwargs):
    from celery.task.sets import subtask
    return subtask(*args, **kwargs)


class PeriodicTask(Task):
    """A periodic task is a task that behaves like a :manpage:`cron` job.

    Results of periodic tasks are not stored by default.

    .. attribute:: run_every

        *REQUIRED* Defines how often the task is run (its interval),
        it can be a :class:`~datetime.timedelta` object, a
        :class:`~celery.schedules.crontab` object or an integer
        specifying the time in seconds.

    .. attribute:: relative

        If set to :const:`True`, run times are relative to the time when the
        server was started. This was the previous behaviour, periodic tasks
        are now scheduled by the clock.

    :raises NotImplementedError: if the :attr:`run_every` attribute is
        not defined.

    Example

        >>> from celery.task import tasks, PeriodicTask
        >>> from datetime import timedelta
        >>> class EveryThirtySecondsTask(PeriodicTask):
        ...     run_every = timedelta(seconds=30)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every 30 seconds")

        >>> from celery.task import PeriodicTask
        >>> from celery.schedules import crontab

        >>> class EveryMondayMorningTask(PeriodicTask):
        ...     run_every = crontab(hour=7, minute=30, day_of_week=1)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every Monday at 7:30AM.")

        >>> class EveryMorningTask(PeriodicTask):
        ...     run_every = crontab(hours=7, minute=30)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every day at 7:30AM.")

        >>> class EveryQuarterPastTheHourTask(PeriodicTask):
        ...     run_every = crontab(minute=15)
        ...
        ...     def run(self, **kwargs):
        ...         logger = self.get_logger(**kwargs)
        ...         logger.info("Execute every 0:15 past the hour every day.")

    """
    abstract = True
    ignore_result = True
    type = "periodic"
    relative = False
    options = None

    def __init__(self):
        app = current_app
        if not hasattr(self, "run_every"):
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")
        self.run_every = maybe_schedule(self.run_every, self.relative)

        # For backward compatibility, add the periodic task to the
        # configuration schedule instead.
        app.conf.CELERYBEAT_SCHEDULE[self.name] = {
                "task": self.name,
                "schedule": self.run_every,
                "args": (),
                "kwargs": {},
                "options": self.options or {},
                "relative": self.relative,
        }

        super(PeriodicTask, self).__init__()

    def timedelta_seconds(self, delta):
        """Convert :class:`~datetime.timedelta` to seconds.

        Doesn't account for negative timedeltas.

        """
        return timeutils.timedelta_seconds(delta)

    def is_due(self, last_run_at):
        """Returns tuple of two items `(is_due, next_time_to_run)`,
        where next time to run is in seconds.

        See :meth:`celery.schedules.schedule.is_due` for more information.

        """
        return self.run_every.is_due(last_run_at)

    def remaining_estimate(self, last_run_at):
        """Returns when the periodic task should run next as a timedelta."""
        return self.run_every.remaining_estimate(last_run_at)

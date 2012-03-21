# -*- coding: utf-8 -*-
"""
    celery.task.base
    ~~~~~~~~~~~~~~~~

    The task implementation has been moved to :mod:`celery.app.task`.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from ..app.task import Context, TaskType, BaseTask  # noqa
from ..schedules import maybe_schedule


class Task(BaseTask):
    abstract = True


class PeriodicTask(BaseTask):
    """A periodic task is a task that adds itself to the
    :setting:`CELERYBEAT_SCHEDULE` setting."""
    abstract = True
    ignore_result = True
    relative = False
    options = None

    def __init__(self):
        if not hasattr(self, "run_every"):
            raise NotImplementedError(
                    "Periodic tasks must have a run_every attribute")
        self.run_every = maybe_schedule(self.run_every, self.relative)
        super(PeriodicTask, self).__init__()

    @classmethod
    def on_bound(cls, app):
        app.conf.CELERYBEAT_SCHEDULE[cls.name] = {
                "task": cls.name,
                "schedule": cls.run_every,
                "args": (),
                "kwargs": {},
                "options": cls.options or {},
                "relative": cls.relative,
        }

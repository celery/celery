# -*- coding: utf-8 -*-
"""
    celery.task.base
    ~~~~~~~~~~~~~~~~

    The task implementation has been moved to :mod:`celery.app.task`.

    This contains the backward compatible Task class used in the old API.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from celery import current_app
from celery.__compat__ import class_property, reclassmethod
from celery.app.task import Context, TaskType, BaseTask  # noqa
from celery.schedules import maybe_schedule

#: list of methods that must be classmethods in the old API.
_COMPAT_CLASSMETHODS = (
    "get_logger", "establish_connection", "get_publisher", "get_consumer",
    "delay", "apply_async", "retry", "apply", "AsyncResult", "subtask",
    "bind", "on_bound", "_get_app", "annotate")


class Task(BaseTask):
    abstract = True
    __bound__ = False

    # In old Celery the @task decorator didn't exist, so one would create
    # classes instead and use them directly (e.g. MyTask.apply_async()).
    # the use of classmethods was a hack so that it was not necessary
    # to instantiate the class before using it, but it has only
    # given us pain (like all magic).
    for name in _COMPAT_CLASSMETHODS:
        locals()[name] = reclassmethod(getattr(BaseTask, name))
    app = class_property(_get_app, bind)  # noqa


class PeriodicTask(Task):
    """A periodic task is a task that adds itself to the
    :setting:`CELERYBEAT_SCHEDULE` setting."""
    abstract = True
    ignore_result = True
    relative = False
    options = None
    compat = True

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


def task(*args, **kwargs):
    """Decorator to create a task class out of any callable.

    **Examples**

    .. code-block:: python

        @task
        def refresh_feed(url):
            return Feed.objects.get(url=url).refresh()

    With setting extra options and using retry.

    .. code-block:: python

        @task(max_retries=10)
        def refresh_feed(url):
            try:
                return Feed.objects.get(url=url).refresh()
            except socket.error, exc:
                refresh_feed.retry(exc=exc)

    Calling the resulting task:

            >>> refresh_feed("http://example.com/rss") # Regular
            <Feed: http://example.com/rss>
            >>> refresh_feed.delay("http://example.com/rss") # Async
            <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>
    """
    return current_app.task(*args, **dict({"accept_magic_kwargs": False,
                                           "base": Task}, **kwargs))


def periodic_task(*args, **options):
    """Decorator to create a task class out of any callable.

        .. admonition:: Examples

            .. code-block:: python

                @task
                def refresh_feed(url):
                    return Feed.objects.get(url=url).refresh()

            With setting extra options and using retry.

            .. code-block:: python

                from celery.task import current

                @task(exchange="feeds")
                def refresh_feed(url):
                    try:
                        return Feed.objects.get(url=url).refresh()
                    except socket.error, exc:
                        current.retry(exc=exc)

            Calling the resulting task:

                >>> refresh_feed("http://example.com/rss") # Regular
                <Feed: http://example.com/rss>
                >>> refresh_feed.delay("http://example.com/rss") # Async
                <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>

    """
    return task(**dict({"base": PeriodicTask}, **options))

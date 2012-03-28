# -*- coding: utf-8 -*-
"""
    celery.task
    ~~~~~~~~~~~

    Creating tasks, subtasks, sets and chords.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .. import current_app
from ..app import app_or_default, current_task as _current_task
from ..local import Proxy
from ..utils import uuid

from .base import BaseTask, Task, PeriodicTask  # noqa
from .sets import group, TaskSet, subtask       # noqa
from .chords import chord                       # noqa
from .control import discard_all                # noqa

current = Proxy(_current_task)


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
    kwargs.setdefault("accept_magic_kwargs", False)
    return app_or_default().task(*args, **kwargs)


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

backend_cleanup = Proxy(lambda: current_app.tasks["celery.backend_cleanup"])


class chain(object):

    def __init__(self, *tasks):
        self.tasks = tasks

    def apply_async(self, **kwargs):
        tasks = [task.clone(task_id=uuid(), **kwargs)
                    for task in self.tasks]
        reduce(lambda a, b: a.link(b), tasks)
        tasks[0].apply_async()
        results = [task.type.AsyncResult(task.options["task_id"])
                        for task in tasks]
        reduce(lambda a, b: a.set_parent(b), reversed(results))
        return results[-1]

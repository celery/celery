# -*- coding: utf-8 -*-
import warnings

from celery.app import app_or_default
from celery.task.base import Task, PeriodicTask
from celery.task.sets import TaskSet, subtask
from celery.task.control import discard_all

__all__ = ["Task", "TaskSet", "PeriodicTask", "subtask", "discard_all"]


def task(*args, **kwargs):
    """Decorator to create a task class out of any callable.

    **Examples**

    .. code-block:: python

        @task()
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

                @task()
                def refresh_feed(url):
                    return Feed.objects.get(url=url).refresh()

            With setting extra options and using retry.

            .. code-block:: python

                @task(exchange="feeds")
                def refresh_feed(url, **kwargs):
                    try:
                        return Feed.objects.get(url=url).refresh()
                    except socket.error, exc:
                        refresh_feed.retry(args=[url], kwargs=kwargs, exc=exc)

            Calling the resulting task:

                >>> refresh_feed("http://example.com/rss") # Regular
                <Feed: http://example.com/rss>
                >>> refresh_feed.delay("http://example.com/rss") # Async
                <AsyncResult: 8998d0f4-da0b-4669-ba03-d5ab5ac6ad5d>

    """
    return task(**dict({"base": PeriodicTask}, **options))


@task(name="celery.backend_cleanup")
def backend_cleanup():
    backend_cleanup.backend.cleanup()


class PingTask(Task):  # ✞
    name = "celery.ping"

    def run(self, **kwargs):
        return "pong"


def ping():  # ✞
    """Deprecated and scheduled for removal in Celery 2.3.

    Please use :meth:`celery.task.control.ping` instead.

    """
    warnings.warn(DeprecationWarning(
        "The ping task has been deprecated and will be removed in Celery "
        "v2.3.  Please use inspect.ping instead."))
    return PingTask.apply_async().get()

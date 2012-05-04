# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

from celery.app.state import get_current_task
from celery.canvas import subtask, maybe_subtask  # noqa
from celery.utils import uuid
from celery.utils.compat import UserList


class TaskSet(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks have been completed.

    :param tasks: A list of :class:`subtask` instances.

    Example::

        >>> urls = ("http://cnn.com/rss", "http://bbc.co.uk/rss")
        >>> s = TaskSet(refresh_feed.s(url) for url in urls)
        >>> taskset_result = s.apply_async()
        >>> list_of_return_values = taskset_result.join()  # *expensive*

    """
    _app = None

    def __init__(self, tasks=None, app=None, Publisher=None):
        self._app = app or self._app
        self.data = [maybe_subtask(t) for t in tasks or []]
        self._Publisher = Publisher

    def apply_async(self, connection=None, connect_timeout=None,
            publisher=None, taskset_id=None):
        """Apply TaskSet."""
        app = self.app

        if app.conf.CELERY_ALWAYS_EAGER:
            return self.apply(taskset_id=taskset_id)

        with app.default_connection(connection, connect_timeout) as conn:
            setid = taskset_id or uuid()
            pub = publisher or self.Publisher(conn)
            results = self._async_results(setid, pub)

            result = app.TaskSetResult(setid, results)
            parent = get_current_task()
            if parent:
                parent.request.children.append(result)
            return result

    def _async_results(self, taskset_id, publisher):
        return [task.apply_async(taskset_id=taskset_id, publisher=publisher)
                for task in self.tasks]

    def apply(self, taskset_id=None):
        """Applies the TaskSet locally by blocking until all tasks return."""
        setid = taskset_id or uuid()
        return self.app.TaskSetResult(setid, self._sync_results(setid))

    def _sync_results(self, taskset_id):
        return [task.apply(taskset_id=taskset_id) for task in self.tasks]

    @property
    def total(self):
        """Number of subtasks in this TaskSet."""
        return len(self)

    def _get_app(self):
        return self._app or self.data[0].type._get_app()

    def _set_app(self, app):
        self._app = app
    app = property(_get_app, _set_app)

    def _get_tasks(self):
        return self.data

    def _set_tasks(self, tasks):
        self.data = tasks
    tasks = property(_get_tasks, _set_tasks)

    def _get_Publisher(self):
        return self._Publisher or self.app.amqp.TaskProducer

    def _set_Publisher(self, Publisher):
        self._Publisher = Publisher
    Publisher = property(_get_Publisher, _set_Publisher)

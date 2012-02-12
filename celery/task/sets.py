# -*- coding: utf-8 -*-
"""
    celery.task.sets
    ~~~~~~~~~~~~~~~~

    Creating and applying groups of tasks.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

from .. import registry
from ..app import app_or_default
from ..datastructures import AttributeDict
from ..utils import cached_property, reprcall, uuid
from ..utils.compat import UserList


class subtask(AttributeDict):
    """Class that wraps the arguments and execution options
    for a single task invocation.

    Used as the parts in a :class:`TaskSet` or to safely
    pass tasks around as callbacks.

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to
      :func:`celery.execute.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = subtask("tasks.add", args=(2, 2))
        >>> subtask(s)
        {"task": "tasks.add", args=(2, 2), kwargs={}, options={}}

    """

    def __init__(self, task=None, args=None, kwargs=None, options=None, **ex):
        init = super(subtask, self).__init__

        if isinstance(task, dict):
            return init(task)  # works like dict(d)

        # Also supports using task class/instance instead of string name.
        try:
            task_name = task.name
        except AttributeError:
            task_name = task

        init(task=task_name, args=tuple(args or ()),
                             kwargs=dict(kwargs or {}, **ex),
                             options=options or {})

    def delay(self, *argmerge, **kwmerge):
        """Shortcut to `apply_async(argmerge, kwargs)`."""
        return self.apply_async(args=argmerge, kwargs=kwmerge)

    def apply(self, args=(), kwargs={}, **options):
        """Apply this task locally."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply(args, kwargs, **options)

    def apply_async(self, args=(), kwargs={}, **options):
        """Apply this task asynchronously."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply_async(args, kwargs, **options)

    def __reduce__(self):
        # for serialization, the task type is lazily loaded,
        # and not stored in the dict itself.
        return (self.__class__, (dict(self), ), None)

    def __repr__(self):
        return reprcall(self["task"], self["args"], self["kwargs"])

    @cached_property
    def type(self):
        return registry.tasks[self.task]


def maybe_subtask(t):
    if not isinstance(t, subtask):
        return subtask(t)
    return t


class TaskSet(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks have been completed.

    :param tasks: A list of :class:`subtask` instances.

    Example::

        >>> urls = ("http://cnn.com/rss", "http://bbc.co.uk/rss")
        >>> taskset = TaskSet(refresh_feed.subtask((url, )) for url in urls)
        >>> taskset_result = taskset.apply_async()
        >>> list_of_return_values = taskset_result.join()  # *expensive*

    """
    #: Total number of subtasks in this set.
    total = None

    def __init__(self, tasks=None, app=None, Publisher=None):
        self.app = app_or_default(app)
        self.data = [maybe_subtask(t) for t in tasks or []]
        self.total = len(self.tasks)
        self.Publisher = Publisher or self.app.amqp.TaskPublisher

    def apply_async(self, connection=None, connect_timeout=None,
            publisher=None, taskset_id=None):
        """Apply taskset."""
        app = self.app

        if app.conf.CELERY_ALWAYS_EAGER:
            return self.apply(taskset_id=taskset_id)

        with app.default_connection(connection, connect_timeout) as conn:
            setid = taskset_id or uuid()
            pub = publisher or self.Publisher(connection=conn)
            try:
                results = self._async_results(setid, pub)
            finally:
                if not publisher:  # created by us.
                    pub.close()

            return app.TaskSetResult(setid, results)

    def _async_results(self, taskset_id, publisher):
        return [task.apply_async(taskset_id=taskset_id, publisher=publisher)
                for task in self.tasks]

    def apply(self, taskset_id=None):
        """Applies the taskset locally by blocking until all tasks return."""
        setid = taskset_id or uuid()
        return self.app.TaskSetResult(setid, self._sync_results(setid))

    def _sync_results(self, taskset_id):
        return [task.apply(taskset_id=taskset_id) for task in self.tasks]

    def _get_tasks(self):
        return self.data

    def _set_tasks(self, tasks):
        self.data = tasks
    tasks = property(_get_tasks, _set_tasks)
group = TaskSet

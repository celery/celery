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

from itertools import chain

from kombu.utils import reprcall

from .. import current_app
from ..app import current_task
from ..datastructures import AttributeDict
from ..utils import cached_property, uuid
from ..utils.functional import maybe_list
from ..utils.compat import UserList, chain_from_iterable


class subtask(AttributeDict):
    """Class that wraps the arguments and execution options
    for a single task invocation.

    Used as the parts in a :class:`TaskSet` or to safely
    pass tasks around as callbacks.

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to :meth:`Task.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = subtask("tasks.add", args=(2, 2))
        >>> subtask(s)
        {"task": "tasks.add", args=(2, 2), kwargs={}, options={}}

    """

    def __init__(self, task=None, args=None, kwargs=None, options=None,
                type=None, **ex):
        init = super(subtask, self).__init__

        if isinstance(task, dict):
            return init(task)  # works like dict(d)

        # Also supports using task class/instance instead of string name.
        try:
            task_name = task.name
            self._type = task
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

    def clone(self, args=(), kwargs={}, **options):
        return self.__class__(self.task,
                              args=tuple(args) + tuple(self.args),
                              kwargs=dict(self.kwargs, **kwargs),
                              options=dict(self.options, **options))

    def apply_async(self, args=(), kwargs={}, **options):
        """Apply this task asynchronously."""
        # For callbacks: extra args are prepended to the stored args.
        args = tuple(args) + tuple(self.args)
        kwargs = dict(self.kwargs, **kwargs)
        options = dict(self.options, **options)
        return self.type.apply_async(args, kwargs, **options)

    def link(self, callback):
        """Add a callback task to be applied if this task
        executes successfully."""
        self.options.setdefault("link", []).append(callback)
        return callback

    def link_error(self, errback):
        """Add a callback task to be applied if an error occurs
        while executing this task."""
        self.options.setdefault("link_error", []).append(errback)
        return errback

    def flatten_links(self):
        """Gives a recursive list of dependencies (unchain if you will,
        but with links intact)."""
        return list(chain_from_iterable(chain([[self]],
                (link.flatten_links()
                    for link in maybe_list(self.options.get("link")) or []))))

    def __reduce__(self):
        # for serialization, the task type is lazily loaded,
        # and not stored in the dict itself.
        return (self.__class__, (dict(self), ), None)

    def __repr__(self):
        return reprcall(self["task"], self["args"], self["kwargs"])

    @cached_property
    def type(self):
        return self._type or current_app.tasks[self.task]


def maybe_subtask(t):
    if not isinstance(t, subtask):
        return subtask(t)
    return t


class group(UserList):
    """A task containing several subtasks, making it possible
    to track how many, or when all of the tasks have been completed.

    :param tasks: A list of :class:`subtask` instances.

    Example::

        >>> urls = ("http://cnn.com/rss", "http://bbc.co.uk/rss")
        >>> g = group(refresh_feed.subtask((url, )) for url in urls)
        >>> group_result = g.apply_async()
        >>> list_of_return_values = group_result.join()  # *expensive*

    """

    def __init__(self, tasks=None, app=None, Publisher=None):
        self._app = app
        self.data = [maybe_subtask(t) for t in tasks or []]
        self._Publisher = Publisher

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

            result = app.TaskSetResult(setid, results)
            parent = current_task()
            if parent:
                parent.request.children.append(result)
            return result

    def _async_results(self, taskset_id, publisher):
        return [task.apply_async(taskset_id=taskset_id, publisher=publisher)
                for task in self.tasks]

    def apply(self, taskset_id=None):
        """Applies the taskset locally by blocking until all tasks return."""
        setid = taskset_id or uuid()
        return self.app.TaskSetResult(setid, self._sync_results(setid))

    def _sync_results(self, taskset_id):
        return [task.apply(taskset_id=taskset_id) for task in self.tasks]

    @property
    def total(self):
        """Number of subtasks in this group."""
        return len(self)

    def _get_app(self):
        return self._app or self.data[0].type.app

    def _set_app(self, app):
        self._app = app
    app = property(_get_app, _set_app)

    def _get_tasks(self):
        return self.data

    def _set_tasks(self, tasks):
        self.data = tasks
    tasks = property(_get_tasks, _set_tasks)

    def _get_Publisher(self):
        return self._Publisher or self.app.amqp.TaskPublisher

    def _set_Publisher(self, Publisher):
        self._Publisher = Publisher
    Publisher = property(_get_Publisher, _set_Publisher)
TaskSet = group

# -*- coding: utf-8 -*-
"""
    celery.registry
    ~~~~~~~~~~~~~~~

    Registry of available tasks.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import inspect

from .exceptions import NotRegistered


class TaskRegistry(dict):
    NotRegistered = NotRegistered

    def regular(self):
        """Get all regular task types."""
        return self.filter_types("regular")

    def periodic(self):
        """Get all periodic task types."""
        return self.filter_types("periodic")

    def register(self, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if not already an
        instance.

        """
        self[task.name] = inspect.isclass(task) and task() or task

    def unregister(self, name):
        """Unregister task by name.

        :param name: name of the task to unregister, or a
            :class:`celery.task.base.Task` with a valid `name` attribute.

        :raises celery.exceptions.NotRegistered: if the task has not
            been registered.

        """
        try:
            # Might be a task class
            name = name.name
        except AttributeError:
            pass
        self.pop(name)

    def filter_types(self, type):
        """Return all tasks of a specific type."""
        return dict((name, task) for name, task in self.iteritems()
                                    if task.type == type)

    def pop(self, key, *args):
        try:
            return dict.pop(self, key, *args)
        except KeyError:
            raise self.NotRegistered(key)


#: Global task registry.
tasks = TaskRegistry()


def _unpickle_task(name):
    return tasks[name]

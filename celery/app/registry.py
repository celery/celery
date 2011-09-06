# -*- coding: utf-8 -*-
"""
    celery.app.registry
    ~~~~~~~~~~~~~~~~~~~

    Registry of available tasks.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import inspect

from .. import current_app
from ..exceptions import NotRegistered


class TaskRegistry(dict):
    NotRegistered = NotRegistered

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

    def pop(self, key, *args):
        try:
            return dict.pop(self, key, *args)
        except KeyError:
            raise self.NotRegistered(key)


def _unpickle_task(name):
    return current_app.tasks[name]

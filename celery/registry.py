"""celery.registry"""
import inspect
from UserDict import UserDict

from celery import discovery
from celery.exceptions import NotRegistered, AlreadyRegistered


class TaskRegistry(UserDict):
    """Site registry for tasks."""

    NotRegistered = NotRegistered

    def __init__(self):
        self.data = {}

    def autodiscover(self):
        """Autodiscovers tasks using :func:`celery.discovery.autodiscover`."""
        discovery.autodiscover()

    def register(self, task):
        """Register a task in the task registry.

        The task will be automatically instantiated if it's a class
        not an instance.
        """

        task = inspect.isclass(task) and task() or task
        name = task.name
        self.data[name] = task

    def unregister(self, name):
        """Unregister task by name.

        :param name: name of the task to unregister, or a
            :class:`celery.task.Task` class with a valid ``name`` attribute.

        :raises celery.exceptions.NotRegistered: if the task has not
            been registered.

        """
        if hasattr(name, "run"):
            name = name.name
        self.pop(name)

    def get_all(self):
        """Get all task types."""
        return self.data

    def filter_types(self, type):
        """Return all tasks of a specific type."""
        return dict((task_name, task)
                        for task_name, task in self.data.items()
                            if task.type == type)

    def get_all_regular(self):
        """Get all regular task types."""
        return self.filter_types(type="regular")

    def get_all_periodic(self):
        """Get all periodic task types."""
        return self.filter_types(type="periodic")

    def get_task(self, name):
        """Get task by name."""
        return self.data[name]

    def __getitem__(self, key):
        try:
            return UserDict.__getitem__(self, key)
        except KeyError, exc:
            raise self.NotRegistered(exc)

    def pop(self, key, *args):
        try:
            return UserDict.pop(self, key, *args)
        except KeyError, exc:
            raise self.NotRegistered(exc)

"""
.. data:: tasks

    The global task registry.

"""
tasks = TaskRegistry()

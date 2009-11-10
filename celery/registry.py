"""celery.registry"""
from celery import discovery
from celery.exceptions import NotRegistered, AlreadyRegistered
from UserDict import UserDict


class TaskRegistry(UserDict):
    """Site registry for tasks."""

    AlreadyRegistered = AlreadyRegistered
    NotRegistered = NotRegistered

    def __init__(self):
        self.data = {}

    def autodiscover(self):
        """Autodiscovers tasks using :func:`celery.discovery.autodiscover`."""
        discovery.autodiscover()

    def register(self, task, name=None):
        """Register a task in the task registry.

        Task can either be a regular function, or a class inheriting
        from :class:`celery.task.Task`.

        :keyword name: By default the :attr:`Task.name` attribute on the
            task is used as the name of the task, but you can override it
            using this option.

        :raises AlreadyRegistered: if the task is already registered.

        """
        is_class = hasattr(task, "run")
        if is_class:
            task = task() # instantiate Task class
        if not name:
            name = getattr(task, "name")

        if name in self.data:
            raise self.AlreadyRegistered(
                    "Task with name %s is already registered." % name)

        if not is_class:
            task.name = name
            task.type = "regular"

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
        if name not in self.data:
            raise self.NotRegistered(
                    "Task with name %s is not registered." % name)
        del self.data[name]

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

"""
.. data:: tasks

    The global task registry.

"""
tasks = TaskRegistry()

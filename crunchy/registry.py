from crunchy import discovery
from UserDict import UserDict


class NotRegistered(Exception):
    """The task is not registered."""


class AlreadyRegistered(Exception):
    """The task is already registered."""


class TaskRegistry(UserDict):
    """Site registry for tasks."""

    AlreadyRegistered = AlreadyRegistered
    NotRegistered = NotRegistered

    def __init__(self):
        self.data = {}

    def autodiscover(self):
        discovery.autodiscover()

    def register(self, task_name, task_func):
        if task_name in self.data:
            raise self.AlreadyRegistered(
                    "Task with name %s is already registered." % task_name)
        
        self.data[task_name] = task_func

    def unregister(self, task_name):
        if task_name not in self.data:
            raise self.NotRegistered(
                    "Task with name %s is not registered." % task_name)
        del self.data[task_name]

    def get_all(self):
        """Get all task types."""
        return self.data

    def get_task(self, task_name):
        """Get task by name."""
        return self.data[task_name]

"""This is the global task registry."""
tasks = TaskRegistry()

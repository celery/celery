"""celery.backends.database"""
from celery.models import TaskMeta, PeriodicTaskMeta
from celery.backends.base import BaseBackend


class Backend(BaseBackend):
    """The database backends. Using Django models to store task metadata."""

    capabilities = ["ResultStore", "PeriodicStatus"]

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}

    def init_periodic_tasks(self):
        """Create entries for all periodic tasks in the database."""
        PeriodicTaskMeta.objects.init_entries()

    def run_periodic_tasks(self):
        """Run all waiting periodic tasks.

        :returns: a list of ``(task, task_id)`` tuples containing
            the task class and id for the resulting tasks applied.

        """
        waiting_tasks = PeriodicTaskMeta.objects.get_waiting_tasks()
        task_id_tuples = []
        for waiting_task in waiting_tasks:
            task_id = waiting_task.delay()
            task_id_tuples.append((waiting_task, task_id))
        return task_id_tuples

    def store_result(self, task_id, result, status):
        """Mark task as done (executed)."""
        if status == "DONE":
            result = self.prepare_result(result)
        elif status == "FAILURE":
            result = self.prepare_exception(result)
        TaskMeta.objects.store_result(task_id, result, status)
        return result

    def is_done(self, task_id):
        """Returns ``True`` if task with ``task_id`` has been executed."""
        return self.get_status(task_id) == "DONE"

    def get_status(self, task_id):
        """Get the status of a task."""
        return self._get_task_meta_for(task_id).status

    def get_result(self, task_id):
        """Get the result for a task."""
        meta = self._get_task_meta_for(task_id)
        if meta.status == "FAILURE":
            return self.exception_to_python(meta.result)
        else:
            return meta.result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        if task_id in self._cache:
            return self._cache[task_id]
        meta = TaskMeta.objects.get_task(task_id)
        if meta.status == "DONE":
            self._cache[task_id] = meta
        return meta

    def cleanup(self):
        """Delete expired metadata."""
        TaskMeta.objects.delete_expired()

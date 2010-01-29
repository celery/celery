from celery.models import TaskMeta, TaskSetMeta
from celery.backends.base import BaseBackend


class DatabaseBackend(BaseBackend):
    """The database backends. Using Django models to store task metadata."""

    capabilities = ["ResultStore"]

    def __init__(self, *args, **kwargs):
        super(DatabaseBackend, self).__init__(*args, **kwargs)
        self._cache = {}

    def store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        result = self.encode_result(result, status)
        TaskMeta.objects.store_result(task_id, result, status,
                                      traceback=traceback)
        return result

    def store_taskset(self, taskset_id, result):
        """Store the result of an executed taskset."""
        TaskSetMeta.objects.store_result(taskset_id, result)
        return result

    def is_successful(self, task_id):
        """Returns ``True`` if task with ``task_id`` has been executed."""
        return self.get_status(task_id) == "SUCCESS"

    def get_status(self, task_id):
        """Get the status of a task."""
        return self._get_task_meta_for(task_id).status

    def get_traceback(self, task_id):
        """Get the traceback of a failed task."""
        return self._get_task_meta_for(task_id).traceback

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
        if meta.status == "SUCCESS":
            self._cache[task_id] = meta
        return meta

    def get_taskset(self, taskset_id):
        """Get the result for a taskset."""
        meta = self._get_taskset_meta_for(taskset_id)
        if meta:
            return meta.result

    def _get_taskset_meta_for(self, taskset_id):
        """Get taskset metadata for a taskset by id."""
        if taskset_id in self._cache:
            return self._cache[taskset_id]
        meta = TaskSetMeta.objects.get_taskset(taskset_id)
        if meta:
            self._cache[taskset_id] = meta
            return meta

    def cleanup(self):
        """Delete expired metadata."""
        TaskMeta.objects.delete_expired()
        TaskSetMeta.objects.delete_expired()

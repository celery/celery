from celery import states
from celery.models import TaskMeta, TaskSetMeta
from celery.backends.base import BaseDictBackend


class DatabaseBackend(BaseDictBackend):
    """The database backends. Using Django models to store task metadata."""

    def _store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        TaskMeta.objects.store_result(task_id, result, status,
                                      traceback=traceback)
        return result

    def _store_taskset(self, taskset_id, result):
        """Store the result of an executed taskset."""
        TaskSetMeta.objects.store_result(taskset_id, result)
        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        if task_id in self._cache:
            return self._cache[task_id]
        meta = TaskMeta.objects.get_task(task_id)
        if meta:
            meta = meta.to_dict()
            if meta["status"] == states.SUCCESS:
                self._cache[task_id] = meta
            return meta

    def _get_taskset_meta_for(self, taskset_id):
        """Get taskset metadata for a taskset by id."""
        if taskset_id in self._cache:
            return self._cache[taskset_id]
        meta = TaskSetMeta.objects.get_taskset(taskset_id)
        if meta:
            meta = self._cache[taskset_id] = meta.to_dict()
            return meta

    def cleanup(self):
        """Delete expired metadata."""
        TaskMeta.objects.delete_expired()
        TaskSetMeta.objects.delete_expired()

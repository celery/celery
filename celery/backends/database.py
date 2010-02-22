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

    def _save_taskset(self, taskset_id, result):
        """Store the result of an executed taskset."""
        TaskSetMeta.objects.store_result(taskset_id, result)
        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        meta = TaskMeta.objects.get_task(task_id)
        if meta:
            return meta.to_dict()

    def _restore_taskset(self, taskset_id):
        """Get taskset metadata for a taskset by id."""
        meta = TaskSetMeta.objects.restore_taskset(taskset_id)
        if meta:
            return meta.to_dict()

    def cleanup(self):
        """Delete expired metadata."""
        TaskMeta.objects.delete_expired()
        TaskSetMeta.objects.delete_expired()

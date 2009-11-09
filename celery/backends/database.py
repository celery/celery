"""celery.backends.database"""
from celery.models import TaskMeta
from celery.backends.base import BaseBackend


class Backend(BaseBackend):
    """The database backends. Using Django models to store task metadata."""

    capabilities = ["ResultStore"]

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}

    def store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        if status == "SUCCESS":
            result = self.prepare_result(result)
        elif status in ["FAILURE", "RETRY"]:
            result = self.prepare_exception(result)
        TaskMeta.objects.store_result(task_id, result, status,
                                      traceback=traceback)
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

    def cleanup(self):
        """Delete expired metadata."""
        TaskMeta.objects.delete_expired()

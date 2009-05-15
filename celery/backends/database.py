from celery.models import TaskMeta
from celery.backends.base import BaseBackend


class Backend(BaseBackend):

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}
   
    def mark_as_done(self, task_id, result):
        """Mark task as done (executed)."""
        result = self.prepare_result(result)
        return TaskMeta.objects.mark_as_done(task_id, result)

    def is_done(self, task_id):
        """Returns ``True`` if task with ``task_id`` has been executed."""
        return self.get_status(task_id) == "DONE"

    def get_status(self, task_id):
        """Get the status of a task."""
        return self._get_task_meta_for(task_id).status

    def get_result(self, task_id):
        """Get the result for a task."""
        return self._get_task_meta_for(task_id).result

    def _get_task_meta_for(self, task_id):
        if task_id in self._cache:
            return self._cache[task_id]
        meta = TaskMeta.objects.get_task(task_id)
        if meta.status == "DONE":
            self._cache[task_id] = meta
        return meta

    def cleanup(self):
        TaskMeta.objects.delete_expired()

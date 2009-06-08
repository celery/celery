"""celery.backends.cache"""
from django.core.cache import cache
from celery.backends.base import BaseBackend
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Backend(BaseBackend):
    """Backend using the Django cache framework to store task metadata."""

    capabilities = ["ResultStore"]

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}

    def _cache_key(self, task_id):
        """Get the cache key for a task by id."""
        return "celery-task-meta-%s" % task_id

    def store_result(self, task_id, result, status):
        """Store task result and status."""
        if status == "DONE":
            result = self.prepare_result(result)
        elif status == "FAILURE":
            result = self.prepare_exception(result)
        meta = {"status": status, "result": pickle.dumps(result)}
        cache.set(self._cache_key(task_id), meta)

    def get_status(self, task_id):
        """Get the status of a task."""
        return self._get_task_meta_for(task_id)["status"]

    def get_result(self, task_id):
        """Get the result of a task."""
        meta = self._get_task_meta_for(task_id)
        if meta["status"] == "FAILURE":
            return self.exception_to_python(meta["result"])
        else:
            return meta["result"]

    def is_done(self, task_id):
        """Returns ``True`` if the task has been executed successfully."""
        return self.get_status(task_id) == "DONE"

    def _get_task_meta_for(self, task_id):
        """Get the task metadata for a task by id."""
        if task_id in self._cache:
            return self._cache[task_id]
        meta = cache.get(self._cache_key(task_id))
        if not meta:
            return {"status": "PENDING", "result": None}
        meta["result"] = pickle.loads(meta.get("result", None))
        if meta.get("status") == "DONE":
            self._cache[task_id] = meta
        return meta

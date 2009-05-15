from django.core.cache import cache
from celery.backends.base import BaseBackend
try:
    import cPickle as pickle
except ImportError:
    import pickle


class Backend(BaseBackend):

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}

    def _cache_key(self, task_id):
        return "celery-task-meta-%s" % task_id

    def mark_as_done(self, task_id, result):
        """Mark task as done (executed)."""
        result = self.prepare_result(result)
        meta = {"status": "DONE", "result": pickle.dumps(result)}
        cache.set(self._cache_key(task_id), meta)

    def get_status(self, task_id):
        return self._get_task_meta_for(self, task_id)["status"]

    def get_result(self, task_id):
        return self._get_task_meta_for(self, task_id)["result"]

    def is_done(self, task_id):
        return self.get_status(task_id) == "DONE"

    def _get_task_meta_for(self, task_id):
        if task_id in self._cache:
            return self._cache[task_id]
        meta = cache.get(self._cache_key(task_id))
        if not meta:
            return {"status": "PENDING", "result": None}
        meta["result"] = pickle.loads(meta.get("result", None))
        if meta.get("status") == "DONE":
            self._cache[task_id] = meta
        return meta

"""celery.backends.tyrant"""
from django.core.exceptions import ImproperlyConfigured

try:
    import pytyrant
except ImportError:
    raise ImproperlyConfigured(
            "The Tokyo Tyrant backend requires the pytyrant library.")

from celery.backends.base import BaseBackend
from django.conf import settings
from carrot.messaging import serialize, deserialize
try:
    import cPickle as pickle
except ImportError:
    import pickle

TT_HOST = getattr(settings, "TT_HOST", None)
TT_PORT = getattr(settings, "TT_PORT", None)

if not TT_HOST or not TT_PORT:
    raise ImproperlyConfigured(
            "To use the Tokyo Tyrant backend, you have to "
            "set the TT_HOST and TT_PORT settings in your settings.py")
else:
    def get_server():
        return pytyrant.PyTyrant.open(TT_HOST, TT_PORT)
    

class Backend(BaseBackend):
    """Tokyo Cabinet based task backend store."""

    def __init__(self, *args, **kwargs):
        super(Backend, self).__init__(*args, **kwargs)
        self._cache = {}

    def _cache_key(self, task_id):
        return "celery-task-meta-%s" % task_id

    def store_result(self, task_id, result, status):
        """Store task result and status."""
        result = self.prepare_result(result)
        meta = {"status": status, "result": pickle.dumps(result)}
        get_server()[self._cache_key(task_id)] = serialize(meta)

    def get_status(self, task_id):
        """Get the status for a task."""
        return self._get_task_meta_for(self, task_id)["status"]

    def get_result(self, task_id):
        """Get the result of a task."""
        return self._get_task_meta_for(self, task_id)["result"]

    def is_done(self, task_id):
        """Returns ``True`` if the task executed successfully."""
        return self.get_status(task_id) == "DONE"

    def _get_task_meta_for(self, task_id):
        if task_id in self._cache:
            return self._cache[task_id]
        meta = get_server().get(self._cache_key(task_id))
        if not meta:
            return {"status": "PENDING", "result": None}
        meta = deserialize(meta)
        meta["result"] = pickle.loads(meta.get("result", None))
        if meta.get("status") == "DONE":
            self._cache[task_id] = meta
        return meta

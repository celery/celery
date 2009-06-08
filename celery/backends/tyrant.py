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


class Backend(BaseBackend):
    """Tokyo Cabinet based task backend store.

    .. attribute:: tyrant_host

        The hostname to the Tokyo Tyrant server.

    .. attribute:: tyrant_port

        The port to the Tokyo Tyrant server.

    """
    tyrant_host = None
    tyrant_port = None

    capabilities = ["ResultStore"]

    def __init__(self, tyrant_host=None, tyrant_port=None):
        """Initialize Tokyo Tyrant backend instance.

        Raises :class:`django.core.exceptions.ImproperlyConfigured` if
        :setting:`TT_HOST` or :setting:`TT_PORT` is not set.

        """
        self.tyrant_host = tyrant_host or \
                            getattr(settings, "TT_HOST", self.tyrant_host)
        self.tyrant_port = tyrant_port or \
                            getattr(settings, "TT_PORT", self.tyrant_port)
        if not self.tyrant_host or not self.tyrant_port:
            raise ImproperlyConfigured(
                "To use the Tokyo Tyrant backend, you have to "
                "set the TT_HOST and TT_PORT settings in your settings.py")
        super(Backend, self).__init__()
        self._cache = {}

    def get_server(self):
        """Get :class:`pytyrant.PyTyrant`` instance with the current
        server configuration."""
        return pytyrant.PyTyrant.open(self.tyrant_host, self.tyrant_port)

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
        self.get_server()[self._cache_key(task_id)] = serialize(meta)

    def get_status(self, task_id):
        """Get the status for a task."""
        return self._get_task_meta_for(task_id)["status"]

    def get_result(self, task_id):
        """Get the result of a task."""
        meta = self._get_task_meta_for(task_id)
        if meta["status"] == "FAILURE":
            return self.exception_to_python(meta["result"])
        else:
            return meta["result"]

    def is_done(self, task_id):
        """Returns ``True`` if the task executed successfully."""
        return self.get_status(task_id) == "DONE"

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        if task_id in self._cache:
            return self._cache[task_id]
        meta = self.get_server().get(self._cache_key(task_id))
        if not meta:
            return {"status": "PENDING", "result": None}
        meta = deserialize(meta)
        meta["result"] = pickle.loads(meta.get("result", None))
        if meta.get("status") == "DONE":
            self._cache[task_id] = meta
        return meta

"""celery.backends.base"""
import time

from celery.exceptions import TimeoutError
from celery.serialization import pickle
from celery.serialization import get_pickled_exception
from celery.serialization import get_pickleable_exception

EXCEPTION_STATES = frozenset(["RETRY", "FAILURE"])


class BaseBackend(object):
    """The base backend class. All backends should inherit from this."""

    capabilities = []
    TimeoutError = TimeoutError

    def encode_result(self, result, status):
        if status == "SUCCESS":
            return self.prepare_value(result)
        elif status in EXCEPTION_STATES:
            return self.prepare_exception(result)

    def store_result(self, task_id, result, status):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "store_result is not supported by this backend.")

    def mark_as_done(self, task_id, result):
        """Mark task as successfully executed."""
        return self.store_result(task_id, result, status="SUCCESS")

    def mark_as_failure(self, task_id, exc, traceback=None):
        """Mark task as executed with failure. Stores the execption."""
        return self.store_result(task_id, exc, status="FAILURE",
                                 traceback=traceback)

    def mark_as_retry(self, task_id, exc, traceback=None):
        """Mark task as being retries. Stores the current
        exception (if any)."""
        return self.store_result(task_id, exc, status="RETRY",
                                 traceback=traceback)

    def prepare_exception(self, exc):
        """Prepare exception for serialization."""
        return get_pickleable_exception(exc)

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        return get_pickled_exception(exc)

    def get_status(self, task_id):
        """Get the status of a task."""
        raise NotImplementedError(
                "get_status is not supported by this backend.")

    def prepare_value(self, result):
        """Prepare value for storage."""
        return result

    def get_result(self, task_id):
        """Get the result of a task."""
        raise NotImplementedError(
                "get_result is not supported by this backend.")

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        raise NotImplementedError(
                "get_traceback is not supported by this backend.")

    def is_successful(self, task_id):
        """Returns ``True`` if the task was successfully executed."""
        return self.get_status(task_id) == "SUCCESS"

    def cleanup(self):
        """Backend cleanup. Is run by
        :class:`celery.task.DeleteExpiredTaskMetaTask`."""
        pass

    def wait_for(self, task_id, timeout=None):
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        If ``timeout`` is not ``None``, this raises the
        :class:`celery.exceptions.TimeoutError` exception if the operation
        takes longer than ``timeout`` seconds.

        """

        sleep_inbetween = 0.5
        time_elapsed = 0.0

        while True:
            status = self.get_status(task_id)
            if status == "SUCCESS":
                return self.get_result(task_id)
            elif status == "FAILURE":
                raise self.get_result(task_id)
            # avoid hammering the CPU checking status.
            time.sleep(sleep_inbetween)
            time_elapsed += sleep_inbetween
            if timeout and time_elapsed >= timeout:
                raise TimeoutError("The operation timed out.")

    def process_cleanup(self):
        """Cleanup actions to do at the end of a task worker process.

        See :func:`celery.worker.jail`.

        """
        pass


class KeyValueStoreBackend(BaseBackend):

    capabilities = ["ResultStore"]

    def __init__(self, *args, **kwargs):
        super(KeyValueStoreBackend, self).__init__()
        self._cache = {}

    def get_cache_key_for_task(self, task_id):
        """Get the cache key for a task by id."""
        return "celery-task-meta-%s" % task_id

    def get(self, key):
        raise NotImplementedError("Must implement the get method.")

    def set(self, key, value):
        raise NotImplementedError("Must implement the set method.")

    def store_result(self, task_id, result, status, traceback=None):
        """Store task result and status."""
        result = self.encode_result(result, status)
        meta = {"status": status, "result": result, "traceback": traceback}
        self.set(self.get_cache_key_for_task(task_id), pickle.dumps(meta))
        return result

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

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        meta = self._get_task_meta_for(task_id)
        return meta["traceback"]

    def is_successful(self, task_id):
        """Returns ``True`` if the task executed successfully."""
        return self.get_status(task_id) == "SUCCESS"

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        if task_id in self._cache:
            return self._cache[task_id]
        meta = self.get(self.get_cache_key_for_task(task_id))
        if not meta:
            return {"status": "PENDING", "result": None}
        meta = pickle.loads(str(meta))
        if meta.get("status") == "SUCCESS":
            self._cache[task_id] = meta
        return meta

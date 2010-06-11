"""celery.backends.base"""
import time

from celery import conf
from celery import states
from celery.exceptions import TimeoutError, TaskRevokedError
from celery.serialization import pickle, get_pickled_exception
from celery.serialization import get_pickleable_exception
from celery.datastructures import LocalCache


class BaseBackend(object):
    """The base backend class. All backends should inherit from this."""

    READY_STATES = states.READY_STATES
    UNREADY_STATES = states.UNREADY_STATES
    EXCEPTION_STATES = states.EXCEPTION_STATES

    TimeoutError = TimeoutError

    def __init__(self, *args, **kwargs):
        pass

    def encode_result(self, result, status):
        if status == states.SUCCESS:
            return self.prepare_value(result)
        elif status in self.EXCEPTION_STATES:
            return self.prepare_exception(result)

    def store_result(self, task_id, result, status):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "store_result is not supported by this backend.")

    def mark_as_started(self, task_id):
        """Mark a task as started"""
        return self.store_result(task_id, None, status=states.STARTED)

    def mark_as_done(self, task_id, result):
        """Mark task as successfully executed."""
        return self.store_result(task_id, result, status=states.SUCCESS)

    def mark_as_failure(self, task_id, exc, traceback=None):
        """Mark task as executed with failure. Stores the execption."""
        return self.store_result(task_id, exc, status=states.FAILURE,
                                 traceback=traceback)

    def mark_as_retry(self, task_id, exc, traceback=None):
        """Mark task as being retries. Stores the current
        exception (if any)."""
        return self.store_result(task_id, exc, status=states.RETRY,
                                 traceback=traceback)

    def mark_as_revoked(self, task_id):
        return self.store_result(task_id, TaskRevokedError(),
                                 status=states.REVOKED, traceback=None)

    def prepare_exception(self, exc):
        """Prepare exception for serialization."""
        return get_pickleable_exception(exc)

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        return get_pickled_exception(exc)

    def prepare_value(self, result):
        """Prepare value for storage."""
        return result

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
            if status == states.SUCCESS:
                return self.get_result(task_id)
            elif status in states.PROPAGATE_STATES:
                raise self.get_result(task_id)
            # avoid hammering the CPU checking status.
            time.sleep(sleep_inbetween)
            time_elapsed += sleep_inbetween
            if timeout and time_elapsed >= timeout:
                raise TimeoutError("The operation timed out.")

    def cleanup(self):
        """Backend cleanup. Is run by
        :class:`celery.task.DeleteExpiredTaskMetaTask`."""
        pass

    def process_cleanup(self):
        """Cleanup actions to do at the end of a task worker process."""
        pass

    def get_status(self, task_id):
        """Get the status of a task."""
        raise NotImplementedError(
                "get_status is not supported by this backend.")

    def get_result(self, task_id):
        """Get the result of a task."""
        raise NotImplementedError(
                "get_result is not supported by this backend.")

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        raise NotImplementedError(
                "get_traceback is not supported by this backend.")

    def save_taskset(self, taskset_id, result):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "save_taskset is not supported by this backend.")

    def restore_taskset(self, taskset_id, cache=True):
        """Get the result of a taskset."""
        raise NotImplementedError(
                "restore_taskset is not supported by this backend.")

    def reload_task_result(self, task_id):
        """Reload task result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_task_result is not supported by this backend.")

    def reload_taskset_result(self, task_id):
        """Reload taskset result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_taskset_result is not supported by this backend.")


class BaseDictBackend(BaseBackend):

    def __init__(self, *args, **kwargs):
        super(BaseDictBackend, self).__init__(*args, **kwargs)
        self._cache = LocalCache(limit=kwargs.get("max_cached_results") or
                                 conf.MAX_CACHED_RESULTS)

    def store_result(self, task_id, result, status, traceback=None):
        """Store task result and status."""
        result = self.encode_result(result, status)
        return self._store_result(task_id, result, status, traceback)

    def get_status(self, task_id):
        """Get the status of a task."""
        return self.get_task_meta(task_id)["status"]

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        return self.get_task_meta(task_id)["traceback"]

    def get_result(self, task_id):
        """Get the result of a task."""
        meta = self.get_task_meta(task_id)
        if meta["status"] in self.EXCEPTION_STATES:
            return self.exception_to_python(meta["result"])
        else:
            return meta["result"]

    def get_task_meta(self, task_id, cache=True):
        if cache and task_id in self._cache:
            return self._cache[task_id]

        meta = self._get_task_meta_for(task_id)
        if cache and meta.get("status") == states.SUCCESS:
            self._cache[task_id] = meta
        return meta

    def reload_task_result(self, task_id):
        self._cache[task_id] = self.get_task_meta(task_id, cache=False)

    def reload_taskset_result(self, taskset_id):
        self._cache[taskset_id] = self.get_taskset_meta(taskset_id,
                                                        cache=False)

    def get_taskset_meta(self, taskset_id, cache=True):
        if cache and taskset_id in self._cache:
            return self._cache[taskset_id]

        meta = self._restore_taskset(taskset_id)
        if cache and meta is not None:
            self._cache[taskset_id] = meta
        return meta

    def restore_taskset(self, taskset_id, cache=True):
        """Get the result for a taskset."""
        meta = self.get_taskset_meta(taskset_id, cache=cache)
        if meta:
            return meta["result"]

    def save_taskset(self, taskset_id, result):
        """Store the result of an executed taskset."""
        return self._save_taskset(taskset_id, result)


class KeyValueStoreBackend(BaseDictBackend):

    def get(self, key):
        raise NotImplementedError("Must implement the get method.")

    def set(self, key, value):
        raise NotImplementedError("Must implement the set method.")

    def get_key_for_task(self, task_id):
        """Get the cache key for a task by id."""
        return "celery-task-meta-%s" % task_id

    def get_key_for_taskset(self, task_id):
        """Get the cache key for a task by id."""
        return "celery-taskset-meta-%s" % task_id

    def _store_result(self, task_id, result, status, traceback=None):
        meta = {"status": status, "result": result, "traceback": traceback}
        self.set(self.get_key_for_task(task_id), pickle.dumps(meta))
        return result

    def _save_taskset(self, taskset_id, result):
        meta = {"result": result}
        self.set(self.get_key_for_taskset(taskset_id), pickle.dumps(meta))
        return result

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        meta = self.get(self.get_key_for_task(task_id))
        if not meta:
            return {"status": states.PENDING, "result": None}
        return pickle.loads(str(meta))

    def _restore_taskset(self, taskset_id):
        """Get task metadata for a task by id."""
        meta = self.get(self.get_key_for_taskset(taskset_id))
        if meta:
            meta = pickle.loads(str(meta))
            return meta

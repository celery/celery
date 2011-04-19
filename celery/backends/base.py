"""celery.backends.base"""
import time

from datetime import timedelta

from celery import states
from celery.datastructures import LRUCache
from celery.exceptions import TimeoutError, TaskRevokedError
from celery.utils import timeutils
from celery.utils.serialization import pickle, get_pickled_exception
from celery.utils.serialization import get_pickleable_exception


class BaseBackend(object):
    """Base backend class."""
    READY_STATES = states.READY_STATES
    UNREADY_STATES = states.UNREADY_STATES
    EXCEPTION_STATES = states.EXCEPTION_STATES

    TimeoutError = TimeoutError

    def __init__(self, *args, **kwargs):
        from celery.app import app_or_default
        self.app = app_or_default(kwargs.get("app"))

    def prepare_expires(self, value, type=None):
        if value is None:
            value = self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(value, timedelta):
            value = timeutils.timedelta_seconds(value)
        if value is not None and type:
            return type(value)
        return value

    def encode_result(self, result, status):
        if status in self.EXCEPTION_STATES:
            return self.prepare_exception(result)
        else:
            return self.prepare_value(result)

    def store_result(self, task_id, result, status, traceback=None):
        """Store the result and status of a task."""
        raise NotImplementedError(
                "store_result is not supported by this backend.")

    def mark_as_started(self, task_id, **meta):
        """Mark a task as started"""
        return self.store_result(task_id, meta, status=states.STARTED)

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

    def forget(self, task_id):
        raise NotImplementedError("%s does not implement forget." % (
                    self.__class__))

    def wait_for(self, task_id, timeout=None, propagate=True, interval=0.5):
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        If `timeout` is not :const:`None`, this raises the
        :class:`celery.exceptions.TimeoutError` exception if the operation
        takes longer than `timeout` seconds.

        """

        time_elapsed = 0.0

        while True:
            status = self.get_status(task_id)
            if status == states.SUCCESS:
                return self.get_result(task_id)
            elif status in states.PROPAGATE_STATES:
                result = self.get_result(task_id)
                if propagate:
                    raise result
                return result
            # avoid hammering the CPU checking status.
            time.sleep(interval)
            time_elapsed += interval
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

    def delete_taskset(self, taskset_id):
        raise NotImplementedError(
                "delete_taskset is not supported by this backend.")

    def reload_task_result(self, task_id):
        """Reload task result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_task_result is not supported by this backend.")

    def reload_taskset_result(self, task_id):
        """Reload taskset result, even if it has been previously fetched."""
        raise NotImplementedError(
                "reload_taskset_result is not supported by this backend.")

    def on_chord_part_return(self, task):
        pass

    def on_chord_apply(self, setid, body, **kwargs):
        from celery.registry import tasks
        tasks["celery.chord_unlock"].apply_async((setid, body, ), kwargs,
                                                 countdown=1)

    def __reduce__(self):
        return (self.__class__, ())


class BaseDictBackend(BaseBackend):

    def __init__(self, *args, **kwargs):
        super(BaseDictBackend, self).__init__(*args, **kwargs)
        self._cache = LRUCache(limit=kwargs.get("max_cached_results") or
                                 self.app.conf.CELERY_MAX_CACHED_RESULTS)

    def store_result(self, task_id, result, status, traceback=None, **kwargs):
        """Store task result and status."""
        result = self.encode_result(result, status)
        return self._store_result(task_id, result, status, traceback, **kwargs)

    def forget(self, task_id):
        self._cache.pop(task_id, None)
        self._forget(task_id)

    def _forget(self, task_id):
        raise NotImplementedError("%s does not implement forget." % (
                    self.__class__))

    def get_status(self, task_id):
        """Get the status of a task."""
        return self.get_task_meta(task_id)["status"]

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        return self.get_task_meta(task_id).get("traceback")

    def get_result(self, task_id):
        """Get the result of a task."""
        meta = self.get_task_meta(task_id)
        if meta["status"] in self.EXCEPTION_STATES:
            return self.exception_to_python(meta["result"])
        else:
            return meta["result"]

    def get_task_meta(self, task_id, cache=True):
        if cache:
            try:
                return self._cache[task_id]
            except KeyError:
                pass

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
        if cache:
            try:
                return self._cache[taskset_id]
            except KeyError:
                pass

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

    def delete_taskset(self, taskset_id):
        self._cache.pop(taskset_id, None)
        return self._delete_taskset(taskset_id)


class KeyValueStoreBackend(BaseDictBackend):
    task_keyprefix = "celery-task-meta-"
    taskset_keyprefix = "celery-taskset-meta-"

    def get(self, key):
        raise NotImplementedError("Must implement the get method.")

    def mget(self, keys):
        raise NotImplementedError("Does not support get_many")

    def set(self, key, value):
        raise NotImplementedError("Must implement the set method.")

    def delete(self, key):
        raise NotImplementedError("Must implement the delete method")

    def get_key_for_task(self, task_id):
        """Get the cache key for a task by id."""
        return self.task_keyprefix + task_id

    def get_key_for_taskset(self, taskset_id):
        """Get the cache key for a task by id."""
        return self.taskset_keyprefix + taskset_id

    def _strip_prefix(self, key):
        for prefix in self.task_keyprefix, self.taskset_keyprefix:
            if key.startswith(prefix):
                return key[len(prefix):]
        return key

    def _mget_to_results(self, values, keys):
        if hasattr(values, "items"):
            # client returns dict so mapping preserved.
            return dict((self._strip_prefix(k), pickle.loads(str(v)))
                            for k, v in values.iteritems()
                                if v is not None)
        else:
            # client returns list so need to recreate mapping.
            return dict((keys[i], pickle.loads(str(value)))
                            for i, value in enumerate(values)
                                if value is not None)

    def get_many(self, task_ids, timeout=None, interval=0.5):
        ids = set(task_ids)
        cached_ids = set()
        for task_id in ids:
            try:
                cached = self._cache[task_id]
            except KeyError:
                pass
            else:
                if cached["status"] in states.READY_STATES:
                    yield task_id, cached
                    cached_ids.add(taskid)

        ids ^= cached_ids
        while ids:
            keys = list(ids)
            r = self._mget_to_results(self.mget([self.get_key_for_task(k)
                                                    for k in keys]), keys)
            self._cache.update(r)
            ids ^= set(r.keys())
            for key, value in r.iteritems():
                yield key, value
            time.sleep(interval)  # don't busy loop.

    def _forget(self, task_id):
        self.delete(self.get_key_for_task(task_id))

    def _store_result(self, task_id, result, status, traceback=None):
        meta = {"status": status, "result": result, "traceback": traceback}
        self.set(self.get_key_for_task(task_id), pickle.dumps(meta))
        return result

    def _save_taskset(self, taskset_id, result):
        self.set(self.get_key_for_taskset(taskset_id),
                 pickle.dumps({"result": result}))
        return result

    def _delete_taskset(self, taskset_id):
        self.delete(self.get_key_for_taskset(taskset_id))

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


class DisabledBackend(BaseBackend):

    def store_result(self, *args, **kwargs):
        pass

    def _is_disabled(self, *args, **kwargs):
        raise NotImplementedError("No result backend configured.  "
                "Please see the documentation for more information.")

    wait_for = _is_disabled
    get_status = _is_disabled
    get_result = _is_disabled
    get_traceback = _is_disabled

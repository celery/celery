# -*- coding: utf-8 -*-
"""
    celery.backends.base
    ~~~~~~~~~~~~~~~~~~~~

    Result backend base classes.

    - :class:`BaseBackend` defines the interface.

    - :class:`KeyValueStoreBackend` is a common base class
      using K/V semantics like _get and _put.

"""
from __future__ import absolute_import

import time
import sys

from datetime import timedelta

from kombu import serialization
from kombu.utils.encoding import bytes_to_str, ensure_bytes, from_utf8

from celery import states
from celery.app import current_task
from celery.datastructures import LRUCache
from celery.exceptions import TimeoutError, TaskRevokedError
from celery.five import items
from celery.result import from_serializable, GroupResult
from celery.utils import timeutils
from celery.utils.serialization import (
        get_pickled_exception,
        get_pickleable_exception,
        create_exception_cls,
)

EXCEPTION_ABLE_CODECS = frozenset(['pickle', 'yaml'])
PY3 = sys.version_info >= (3, 0)


def unpickle_backend(cls, args, kwargs):
    """Returns an unpickled backend."""
    return cls(*args, **kwargs)


class BaseBackend(object):
    READY_STATES = states.READY_STATES
    UNREADY_STATES = states.UNREADY_STATES
    EXCEPTION_STATES = states.EXCEPTION_STATES

    TimeoutError = TimeoutError

    #: Time to sleep between polling each individual item
    #: in `ResultSet.iterate`. as opposed to the `interval`
    #: argument which is for each pass.
    subpolling_interval = None

    #: If true the backend must implement :meth:`get_many`.
    supports_native_join = False

    def __init__(self, app=None, serializer=None, max_cached_results=None,
            **kwargs):
        from celery.app import app_or_default
        self.app = app_or_default(app)
        self.serializer = serializer or self.app.conf.CELERY_RESULT_SERIALIZER
        (self.content_type,
         self.content_encoding,
         self.encoder) = serialization.registry._encoders[self.serializer]
        self._cache = LRUCache(limit=max_cached_results or
                                      self.app.conf.CELERY_MAX_CACHED_RESULTS)

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

    def mark_as_revoked(self, task_id, reason=''):
        return self.store_result(task_id, TaskRevokedError(reason),
                                 status=states.REVOKED, traceback=None)

    def prepare_exception(self, exc):
        """Prepare exception for serialization."""
        if self.serializer in EXCEPTION_ABLE_CODECS:
            return get_pickleable_exception(exc)
        return {'exc_type': type(exc).__name__, 'exc_message': str(exc)}

    def exception_to_python(self, exc):
        """Convert serialized exception to Python exception."""
        if self.serializer in EXCEPTION_ABLE_CODECS:
            return get_pickled_exception(exc)
        return create_exception_cls(from_utf8(exc['exc_type']),
                                    sys.modules[__name__])(exc['exc_message'])

    def prepare_value(self, result):
        """Prepare value for storage."""
        if isinstance(result, GroupResult):
            return result.serializable()
        return result

    def encode(self, data):
        _, _, payload = serialization.encode(data, serializer=self.serializer)
        return payload

    def decode(self, payload):
        payload = PY3 and payload or str(payload)
        return serialization.decode(payload,
                                    content_type=self.content_type,
                                    content_encoding=self.content_encoding)

    def wait_for(self, task_id, timeout=None, propagate=True, interval=0.5):
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        If `timeout` is not :const:`None`, this raises the
        :class:`celery.exceptions.TimeoutError` exception if the operation
        takes longer than `timeout` seconds.

        """

        time_elapsed = 0.0

        while 1:
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
                raise TimeoutError('The operation timed out.')

    def prepare_expires(self, value, type=None):
        if value is None:
            value = self.app.conf.CELERY_TASK_RESULT_EXPIRES
        if isinstance(value, timedelta):
            value = timeutils.timedelta_seconds(value)
        if value is not None and type:
            return type(value)
        return value

    def encode_result(self, result, status):
        if status in self.EXCEPTION_STATES and isinstance(result, Exception):
            return self.prepare_exception(result)
        else:
            return self.prepare_value(result)

    def store_result(self, task_id, result, status, traceback=None, **kwargs):
        """Update task state and result."""
        result = self.encode_result(result, status)
        self._store_result(task_id, result, status, traceback, **kwargs)
        return result

    def forget(self, task_id):
        self._cache.pop(task_id, None)
        self._forget(task_id)

    def _forget(self, task_id):
        raise NotImplementedError('backend does not implement forget.')

    def get_status(self, task_id):
        """Get the status of a task."""
        return self.get_task_meta(task_id)['status']

    def get_traceback(self, task_id):
        """Get the traceback for a failed task."""
        return self.get_task_meta(task_id).get('traceback')

    def get_result(self, task_id):
        """Get the result of a task."""
        meta = self.get_task_meta(task_id)
        if meta['status'] in self.EXCEPTION_STATES:
            return self.exception_to_python(meta['result'])
        else:
            return meta['result']

    def get_children(self, task_id):
        """Get the list of subtasks sent by a task."""
        try:
            return self.get_task_meta(task_id)['children']
        except KeyError:
            pass

    def get_task_meta(self, task_id, cache=True):
        if cache:
            try:
                return self._cache[task_id]
            except KeyError:
                pass

        meta = self._get_task_meta_for(task_id)
        if cache and meta.get('status') == states.SUCCESS:
            self._cache[task_id] = meta
        return meta

    def reload_task_result(self, task_id):
        """Reload task result, even if it has been previously fetched."""
        self._cache[task_id] = self.get_task_meta(task_id, cache=False)

    def reload_group_result(self, group_id):
        """Reload group result, even if it has been previously fetched."""
        self._cache[group_id] = self.get_group_meta(group_id, cache=False)

    def get_group_meta(self, group_id, cache=True):
        if cache:
            try:
                return self._cache[group_id]
            except KeyError:
                pass

        meta = self._restore_group(group_id)
        if cache and meta is not None:
            self._cache[group_id] = meta
        return meta

    def restore_group(self, group_id, cache=True):
        """Get the result for a group."""
        meta = self.get_group_meta(group_id, cache=cache)
        if meta:
            return meta['result']

    def save_group(self, group_id, result):
        """Store the result of an executed group."""
        return self._save_group(group_id, result)

    def delete_group(self, group_id):
        self._cache.pop(group_id, None)
        return self._delete_group(group_id)

    def cleanup(self):
        """Backend cleanup. Is run by
        :class:`celery.task.DeleteExpiredTaskMetaTask`."""
        pass

    def process_cleanup(self):
        """Cleanup actions to do at the end of a task worker process."""
        pass

    def on_task_call(self, producer, task_id):
        return {}

    def on_chord_part_return(self, task, propagate=False):
        pass

    def fallback_chord_unlock(self, group_id, body, result=None, **kwargs):
        kwargs['result'] = [r.id for r in result]
        self.app.tasks['celery.chord_unlock'].apply_async((group_id, body, ),
                                                          kwargs, countdown=1)
    on_chord_apply = fallback_chord_unlock

    def current_task_children(self):
        current = current_task()
        if current:
            return [r.serializable() for r in current.request.children]

    def __reduce__(self, args=(), kwargs={}):
        return (unpickle_backend, (self.__class__, args, kwargs))
BaseDictBackend = BaseBackend  # XXX compat


class KeyValueStoreBackend(BaseBackend):
    task_keyprefix = ensure_bytes('celery-task-meta-')
    group_keyprefix = ensure_bytes('celery-taskset-meta-')
    chord_keyprefix = ensure_bytes('chord-unlock-')
    implements_incr = False

    def get(self, key):
        raise NotImplementedError('Must implement the get method.')

    def mget(self, keys):
        raise NotImplementedError('Does not support get_many')

    def set(self, key, value):
        raise NotImplementedError('Must implement the set method.')

    def delete(self, key):
        raise NotImplementedError('Must implement the delete method')

    def incr(self, key):
        raise NotImplementedError('Does not implement incr')

    def expire(self, key, value):
        pass

    def get_key_for_task(self, task_id):
        """Get the cache key for a task by id."""
        return self.task_keyprefix + ensure_bytes(task_id)

    def get_key_for_group(self, group_id):
        """Get the cache key for a group by id."""
        return self.group_keyprefix + ensure_bytes(group_id)

    def get_key_for_chord(self, group_id):
        """Get the cache key for the chord waiting on group with given id."""
        return self.chord_keyprefix + ensure_bytes(group_id)

    def _strip_prefix(self, key):
        """Takes bytes, emits string."""
        key = ensure_bytes(key)
        for prefix in self.task_keyprefix, self.group_keyprefix:
            if key.startswith(prefix):
                return bytes_to_str(key[len(prefix):])
        return bytes_to_str(key)

    def _mget_to_results(self, values, keys):
        if hasattr(values, 'items'):
            # client returns dict so mapping preserved.
            return dict((self._strip_prefix(k), self.decode(v))
                            for k, v in items(values)
                                if v is not None)
        else:
            # client returns list so need to recreate mapping.
            return dict((bytes_to_str(keys[i]), self.decode(value))
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
                if cached['status'] in states.READY_STATES:
                    yield bytes_to_str(task_id), cached
                    cached_ids.add(task_id)

        ids.difference_update(cached_ids)
        iterations = 0
        while ids:
            keys = list(ids)
            r = self._mget_to_results(self.mget([self.get_key_for_task(k)
                                                    for k in keys]), keys)
            self._cache.update(r)
            ids.difference_update(set(map(bytes_to_str, r)))
            for key, value in items(r):
                yield bytes_to_str(key), value
            if timeout and iterations * interval >= timeout:
                raise TimeoutError('Operation timed out ({0})'.format(timeout))
            time.sleep(interval)  # don't busy loop.
            iterations += 1

    def _forget(self, task_id):
        self.delete(self.get_key_for_task(task_id))

    def _store_result(self, task_id, result, status, traceback=None):
        meta = {'status': status, 'result': result, 'traceback': traceback,
                'children': self.current_task_children()}
        self.set(self.get_key_for_task(task_id), self.encode(meta))
        return result

    def _save_group(self, group_id, result):
        self.set(self.get_key_for_group(group_id),
                 self.encode({'result': result.serializable()}))
        return result

    def _delete_group(self, group_id):
        self.delete(self.get_key_for_group(group_id))

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        meta = self.get(self.get_key_for_task(task_id))
        if not meta:
            return {'status': states.PENDING, 'result': None}
        return self.decode(meta)

    def _restore_group(self, group_id):
        """Get task metadata for a task by id."""
        meta = self.get(self.get_key_for_group(group_id))
        # previously this was always pickled, but later this
        # was extended to support other serializers, so the
        # structure is kind of weird.
        if meta:
            meta = self.decode(meta)
            result = meta['result']
            if isinstance(result, (list, tuple)):
                return {'result': from_serializable(result)}
            return meta

    def on_chord_apply(self, group_id, body, result=None, **kwargs):
        if self.implements_incr:
            self.app.GroupResult(group_id, result).save()
        else:
            self.fallback_chord_unlock(group_id, body, result, **kwargs)

    def on_chord_part_return(self, task, propagate=False):
        if not self.implements_incr:
            return
        from celery import subtask
        from celery.result import GroupResult
        gid = task.request.group
        if not gid:
            return
        key = self.get_key_for_chord(gid)
        deps = GroupResult.restore(gid, backend=task.backend)
        if deps is None:
            return
        val = self.incr(key)
        if val >= len(deps):
            subtask(task.request.chord).delay(deps.join(propagate=propagate))
            deps.delete()
            self.client.delete(key)
        else:
            self.expire(key, 86400)


class DisabledBackend(BaseBackend):
    _cache = {}   # need this attribute to reset cache in tests.

    def store_result(self, *args, **kwargs):
        pass

    def _is_disabled(self, *args, **kwargs):
        raise NotImplementedError('No result backend configured.  '
                'Please see the documentation for more information.')
    wait_for = get_status = get_result = get_traceback = _is_disabled

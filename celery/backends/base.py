# -*- coding: utf-8 -*-
"""Result backend base classes.

- :class:`BaseBackend` defines the interface.

- :class:`KeyValueStoreBackend` is a common base class
    using K/V semantics like _get and _put.
"""
import sys
import time
<<<<<<< HEAD
from collections import namedtuple
=======

>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from datetime import timedelta
from numbers import Number
from typing import (
    Any, AnyStr, Callable, Dict, Iterable, Iterator,
    Mapping, MutableMapping, NamedTuple, Optional, Set, Sequence, Tuple,
)
from weakref import WeakValueDictionary

from billiard.einfo import ExceptionInfo
<<<<<<< HEAD
from kombu.serialization import dumps, loads, prepare_accept_content
from kombu.serialization import registry as serializer_registry
=======
from kombu.serialization import (
    dumps, loads, prepare_accept_content,
    registry as serializer_registry,
)
from kombu.types import ProducerT
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu.utils.encoding import bytes_to_str, ensure_bytes, from_utf8
from kombu.utils.url import maybe_sanitize_url

from celery import current_app, group, maybe_signature, states
from celery._state import get_current_task
<<<<<<< HEAD
from celery.exceptions import (ChordError, ImproperlyConfigured,
                               TaskRevokedError, TimeoutError)
from celery.five import items
from celery.result import (GroupResult, ResultBase, allow_join_result,
                           result_from_tuple)
=======
from celery.exceptions import (
    ChordError, TimeoutError, TaskRevokedError, ImproperlyConfigured,
)
from celery.result import (
    GroupResult, ResultBase, allow_join_result, result_from_tuple,
)
from celery.types import AppT, BackendT, ResultT, RequestT, SignatureT
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery.utils.collections import BufferMap
from celery.utils.functional import LRUCache, arity_greater
from celery.utils.log import get_logger
from celery.utils.serialization import (create_exception_cls,
                                        get_pickleable_exception,
                                        get_pickled_exception)

__all__ = ('BaseBackend', 'KeyValueStoreBackend', 'DisabledBackend')

EXCEPTION_ABLE_CODECS = frozenset({'pickle'})

logger = get_logger(__name__)

MESSAGE_BUFFER_MAX = 8192

E_NO_BACKEND = """
No result backend is configured.
Please see the documentation for more information.
"""

E_CHORD_NO_BACKEND = """
Starting chords requires a result backend to be configured.

Note that a group chained with a task is also upgraded to be a chord,
as this pattern requires synchronization.

Result backends that supports chords: Redis, Database, Memcached, and more.
"""


class pending_results_t(NamedTuple):
    """Tuple of concrete and weak references to pending results."""

    concrete: Mapping
    weak: WeakValueDictionary


def unpickle_backend(cls: type, args: Tuple, kwargs: Dict) -> BackendT:
    """Return an unpickled backend."""
    return cls(*args, app=current_app._get_current_object(), **kwargs)


class _nulldict(dict):

    def ignore(self, *a, **kw) -> None:
        ...
    __setitem__ = update = setdefault = ignore


class Backend:

    READY_STATES = states.READY_STATES
    UNREADY_STATES = states.UNREADY_STATES
    EXCEPTION_STATES = states.EXCEPTION_STATES

    TimeoutError = TimeoutError

    #: Time to sleep between polling each individual item
    #: in `ResultSet.iterate`. as opposed to the `interval`
    #: argument which is for each pass.
    subpolling_interval: float = None

    #: If true the backend must implement :meth:`get_many`.
    supports_native_join = False

    #: If true the backend must automatically expire results.
    #: The daily backend_cleanup periodic task won't be triggered
    #: in this case.
    supports_autoexpire = False

    #: Set to true if the backend is peristent by default.
    persistent = True

    retry_policy = {
        'max_retries': 20,
        'interval_start': 0,
        'interval_step': 1,
        'interval_max': 1,
    }

    def __init__(self, app: AppT,
                 *,
                 serializer: str = None,
                 max_cached_results: int = None,
                 accept: Set[str] = None,
                 expires: float = None,
                 expires_type: Callable = None,
                 url: str = None,
                 **kwargs) -> None:
        self.app = app
        conf = self.app.conf
        self.serializer = serializer or conf.result_serializer
        (self.content_type,
         self.content_encoding,
         self.encoder) = serializer_registry._encoders[self.serializer]
        cmax = max_cached_results or conf.result_cache_max
        self._cache = _nulldict() if cmax == -1 else LRUCache(limit=cmax)

        self.expires = self.prepare_expires(expires, expires_type)
        self.accept = prepare_accept_content(
            conf.accept_content if accept is None else accept)
        self._pending_results = pending_results_t({}, WeakValueDictionary())
        self._pending_messages = BufferMap(MESSAGE_BUFFER_MAX)
        self.url = url

    def as_uri(self, include_password: bool = False) -> str:
        """Return the backend as an URI, sanitizing the password or not."""
        # when using maybe_sanitize_url(), "/" is added
        # we're stripping it for consistency
        if include_password:
            return self.url
        url = maybe_sanitize_url(self.url or '')
        return url[:-1] if url.endswith(':///') else url

    async def mark_as_started(self, task_id: str, **meta) -> Any:
        """Mark a task as started."""
        return await self.store_result(task_id, meta, states.STARTED)

    async def mark_as_done(self, task_id: str, result: Any,
                           *,
                           request: RequestT = None,
                           store_result: bool = True,
                           state: str = states.SUCCESS) -> None:
        """Mark task as successfully executed."""
        if store_result:
            await self.store_result(task_id, result, state, request=request)
        if request and request.chord:
            await self.on_chord_part_return(request, state, result)

    async def mark_as_failure(self, task_id: str, exc: Exception,
                              *,
                              traceback: str = None,
                              request: RequestT = None,
                              store_result: bool = True,
                              call_errbacks: bool = True,
                              state: str = states.FAILURE) -> None:
        """Mark task as executed with failure."""
        if store_result:
            await self.store_result(task_id, exc, state,
                                    traceback=traceback, request=request)
        if request:
            if request.chord:
                await self.on_chord_part_return(request, state, exc)
            if call_errbacks and request.errbacks:
                await self._call_task_errbacks(request, exc, traceback)

    async def _call_task_errbacks(self,
                                  request: RequestT,
                                  exc: Exception,
                                  traceback: str) -> None:
        old_signature = []
        for errback in request.errbacks:
            errback = self.app.signature(errback)
            if arity_greater(errback.type.__header__, 1):
                errback(request, exc, traceback)
            else:
                old_signature.append(errback)
        if old_signature:
            # Previously errback was called as a task so we still
            # need to do so if the errback only takes a single task_id arg.
            task_id = request.id
            root_id = request.root_id or task_id
            await group(old_signature, app=self.app).apply_async(
                (task_id,), parent_id=task_id, root_id=root_id
            )

    async def mark_as_revoked(self, task_id: str, reason: str = '',
                              *,
                              request: RequestT = None,
                              store_result: bool = True,
                              state: str = states.REVOKED) -> None:
        exc = TaskRevokedError(reason)
        if store_result:
            await self.store_result(task_id, exc, state,
                                    traceback=None, request=request)
        if request and request.chord:
            await self.on_chord_part_return(request, state, exc)

    async def mark_as_retry(self, task_id: str, exc: Exception,
                            *,
                            traceback: str = None,
                            request: RequestT = None,
                            store_result: bool = True,
                            state: str = states.RETRY) -> None:
        """Mark task as being retries.

        Note:
            Stores the current exception (if any).
        """
        return await self.store_result(task_id, exc, state,
                                       traceback=traceback, request=request)

    async def chord_error_from_stack(self, callback: Callable,
                                     exc: Exception = None) -> ExceptionInfo:
        # need below import for test for some crazy reason
        from celery import group  # pylint: disable
        app = self.app
        try:
            backend = app._tasks[callback.task].backend
        except KeyError:
            backend = self
        try:
            await group(
                [app.signature(errback)
                 for errback in callback.options.get('link_error') or []],
                app=app,
            ).apply_async((callback.id,))
        except Exception as eb_exc:  # pylint: disable=broad-except
            return await backend.fail_from_current_stack(
                callback.id, exc=eb_exc)
        else:
            return await backend.fail_from_current_stack(
                callback.id, exc=exc)

    async def fail_from_current_stack(self, task_id: str,
                                      exc: Exception = None) -> ExceptionInfo:
        type_, real_exc, tb = sys.exc_info()
        try:
            exc = real_exc if exc is None else exc
            ei = ExceptionInfo((type_, exc, tb))
            await self.mark_as_failure(task_id, exc, ei.traceback)
            return ei
        finally:
            del tb

    def prepare_exception(self, exc: Exception, serializer: str = None) -> Any:
        """Prepare exception for serialization."""
        serializer = self.serializer if serializer is None else serializer
        if serializer in EXCEPTION_ABLE_CODECS:
            return get_pickleable_exception(exc)
        return {'exc_type': type(exc).__name__,
                'exc_message': exc.args,
                'exc_module': type(exc).__module__}

    def exception_to_python(self, exc: Any) -> Exception:
        """Convert serialized exception to Python exception."""
        if exc:
            if not isinstance(exc, BaseException):
                exc_module = exc.get('exc_module')
                if exc_module is None:
                    cls = create_exception_cls(
                        from_utf8(exc['exc_type']), __name__)
                else:
                    exc_module = from_utf8(exc_module)
                    exc_type = from_utf8(exc['exc_type'])
                    cls = getattr(sys.modules[exc_module], exc_type)
                exc_msg = exc['exc_message']
                exc = cls(*exc_msg if isinstance(exc_msg, tuple) else exc_msg)
            if self.serializer in EXCEPTION_ABLE_CODECS:
                exc = get_pickled_exception(exc)
        return exc

    def prepare_value(self, result: Any) -> Any:
        """Prepare value for storage."""
        if self.serializer != 'pickle' and isinstance(result, ResultBase):
            return result.as_tuple()
        return result

    def encode(self, data: Any) -> AnyStr:
        _, _, payload = self._encode(data)
        return payload

    def _encode(self, data: Any) -> AnyStr:
        return dumps(data, serializer=self.serializer)

    def meta_from_decoded(self, meta: MutableMapping) -> MutableMapping:
        if meta['status'] in self.EXCEPTION_STATES:
            meta['result'] = self.exception_to_python(meta['result'])
        return meta

    def decode_result(self, payload: AnyStr) -> Mapping:
        return self.meta_from_decoded(self.decode(payload))

    def decode(self, payload: AnyStr) -> Mapping:
        return loads(payload,
                     content_type=self.content_type,
                     content_encoding=self.content_encoding,
                     accept=self.accept)

    def prepare_expires(self, value: Optional[Number],
                        type: Callable = None) -> Optional[Number]:
        if value is None:
            value = self.app.conf.result_expires
        if isinstance(value, timedelta):
            value = value.total_seconds()
        if value is not None and type:
            return type(value)
        return value

    def prepare_persistent(self, enabled: bool = None) -> bool:
        if enabled is not None:
            return enabled
        p = self.app.conf.result_persistent
        return self.persistent if p is None else p

    def encode_result(self, result: Any, state: str) -> Any:
        if state in self.EXCEPTION_STATES and isinstance(result, Exception):
            return self.prepare_exception(result)
        else:
            return self.prepare_value(result)

    def is_cached(self, task_id: str) -> bool:
        return task_id in self._cache

    async def store_result(self, task_id: str, result: Any, state: str,
                           *,
                           traceback: str = None,
                           request: RequestT = None, **kwargs) -> Any:
        """Update task state and result."""
        result = self.encode_result(result, state)
        await self._store_result(task_id, result, state, traceback,
                                 request=request, **kwargs)
        return result

    async def forget(self, task_id: str) -> None:
        self._cache.pop(task_id, None)
        await self._forget(task_id)

    async def _forget(self, task_id: str) -> None:
        raise NotImplementedError('backend does not implement forget.')

    def get_state(self, task_id: str) -> str:
        """Get the state of a task."""
        return self.get_task_meta(task_id)['status']

    def get_traceback(self, task_id: str) -> str:
        """Get the traceback for a failed task."""
        return self.get_task_meta(task_id).get('traceback')

    def get_result(self, task_id: str) -> Any:
        """Get the result of a task."""
        return self.get_task_meta(task_id).get('result')

    def get_children(self, task_id: str) -> Sequence[str]:
        """Get the list of subtasks sent by a task."""
        try:
            return self.get_task_meta(task_id)['children']
        except KeyError:
            pass

    def _ensure_not_eager(self) -> None:
        if self.app.conf.task_always_eager:
            raise RuntimeError(
                "Cannot retrieve result with task_always_eager enabled")

    def get_task_meta(self, task_id: str, cache: bool = True) -> Mapping:
        self._ensure_not_eager()
        if cache:
            try:
                return self._cache[task_id]
            except KeyError:
                pass

        meta = self._get_task_meta_for(task_id)
        if cache and meta.get('status') == states.SUCCESS:
            self._cache[task_id] = meta
        return meta

    def reload_task_result(self, task_id: str) -> None:
        """Reload task result, even if it has been previously fetched."""
        self._cache[task_id] = self.get_task_meta(task_id, cache=False)

    def reload_group_result(self, group_id: str) -> None:
        """Reload group result, even if it has been previously fetched."""
        self._cache[group_id] = self.get_group_meta(group_id, cache=False)

    def get_group_meta(self, group_id: str, cache: bool = True) -> Mapping:
        self._ensure_not_eager()
        if cache:
            try:
                return self._cache[group_id]
            except KeyError:
                pass

        meta = self._restore_group(group_id)
        if cache and meta is not None:
            self._cache[group_id] = meta
        return meta

    def restore_group(self, group_id: str, cache: bool = True) -> GroupResult:
        """Get the result for a group."""
        meta = self.get_group_meta(group_id, cache=cache)
        if meta:
            return meta['result']

    def save_group(self, group_id: str, result: GroupResult) -> GroupResult:
        """Store the result of an executed group."""
        return self._save_group(group_id, result)

    def _save_group(self, group_id: str, result: GroupResult) -> GroupResult:
        raise NotImplementedError()

    def delete_group(self, group_id: str) -> None:
        self._cache.pop(group_id, None)
        self._delete_group(group_id)

    def _delete_group(self, group_id: str) -> None:
        raise NotImplementedError()

    def cleanup(self) -> None:
        """Backend cleanup.

        Note:
            This is run by :class:`celery.task.DeleteExpiredTaskMetaTask`.
        """
        ...

    def process_cleanup(self) -> None:
        """Cleanup actions to do at the end of a task worker process."""
        ...

    async def on_task_call(self, producer: ProducerT, task_id: str) -> Mapping:
        return {}

    async def add_to_chord(self, chord_id: str, result: ResultT) -> None:
        raise NotImplementedError('Backend does not support add_to_chord')

    async def on_chord_part_return(self, request: RequestT,
                                   state: str,
                                   result: Any,
                                   **kwargs) -> None:
        ...

    async def fallback_chord_unlock(self, group_id: str, body: SignatureT,
                                    result: ResultT = None,
                                    countdown: float = 1,
                                    **kwargs) -> None:
        kwargs['result'] = [r.as_tuple() for r in result]
        await self.app.tasks['celery.chord_unlock'].apply_async(
            (group_id, body,), kwargs, countdown=countdown,
        )

    def ensure_chords_allowed(self) -> None:
        ...

    async def apply_chord(self, header: SignatureT, partial_args: Sequence,
                          *,
                          group_id: str, body: SignatureT,
                          options: Mapping = {}, **kwargs) -> ResultT:
        self.ensure_chords_allowed()
        fixed_options = {k: v for k, v in options.items() if k != 'task_id'}
        result = await header(
            *partial_args, task_id=group_id, **fixed_options or {})
        await self.fallback_chord_unlock(group_id, body, **kwargs)
        return result

    def current_task_children(self,
                              request: RequestT = None) -> Sequence[Tuple]:
        request = request or getattr(get_current_task(), 'request', None)
        if request:
            return [r.as_tuple() for r in getattr(request, 'children', [])]

    def __reduce__(self, args: Tuple = (), kwargs: Dict = {}) -> Tuple:
        return (unpickle_backend, (self.__class__, args, kwargs))


class SyncBackendMixin:

    async def iter_native(
            self, result: ResultT,
            *,
            timeout: float = None,
            interval: float = 0.5,
            no_ack: bool = True,
            on_message: Callable = None,
            on_interval: Callable = None) -> Iterable[Tuple[str, Mapping]]:
        self._ensure_not_eager()
        results = result.results
        if not results:
            return iter([])
        return await self.get_many(
            {r.id for r in results},
            timeout=timeout, interval=interval, no_ack=no_ack,
            on_message=on_message, on_interval=on_interval,
        )

    async def wait_for_pending(self, result: ResultT,
                               *,
                               timeout: float = None,
                               interval: float = 0.5,
                               no_ack: bool = True,
                               on_message: Callable = None,
                               on_interval: Callable = None,
                               callback: Callable = None,
                               propagate: bool = True) -> Any:
        self._ensure_not_eager()
        if on_message is not None:
            raise ImproperlyConfigured(
                'Backend does not support on_message callback')

        meta = await self.wait_for(
            result.id, timeout=timeout,
            interval=interval,
            on_interval=on_interval,
            no_ack=no_ack,
        )
        if meta:
            result._maybe_set_cache(meta)
            return result.maybe_throw(propagate=propagate, callback=callback)

    async def wait_for(self, task_id: str,
                       *,
                       timeout: float = None,
                       interval: float = 0.5,
                       no_ack: bool = True,
                       on_interval: Callable = None) -> Mapping:
        """Wait for task and return its result.

        If the task raises an exception, this exception
        will be re-raised by :func:`wait_for`.

        Raises:
            celery.exceptions.TimeoutError:
                If `timeout` is not :const:`None`, and the operation
                takes longer than `timeout` seconds.
        """
        self._ensure_not_eager()

        time_elapsed = 0.0

        while 1:
            meta = self.get_task_meta(task_id)
            if meta['status'] in states.READY_STATES:
                return meta
            if on_interval:
                await on_interval()
            # avoid hammering the CPU checking status.
            time.sleep(interval)
            time_elapsed += interval
            if timeout and time_elapsed >= timeout:
                raise TimeoutError('The operation timed out.')

    def add_pending_result(self, result: ResultT,
                           *,
                           weak: bool = False) -> ResultT:
        return result

    def remove_pending_result(self, result: ResultT) -> ResultT:
        return result

    @property
    def is_async(self) -> bool:
        return False


class BaseBackend(Backend, SyncBackendMixin):
    """Base (synchronous) result backend."""


BaseDictBackend = BaseBackend  # noqa: E305 XXX compat


class BaseKeyValueStoreBackend(Backend):
    key_t = ensure_bytes
    task_keyprefix = 'celery-task-meta-'
    group_keyprefix = 'celery-taskset-meta-'
    chord_keyprefix = 'chord-unlock-'
    implements_incr = False

    def __init__(self, *args, **kwargs) -> None:
        if hasattr(self.key_t, '__func__'):  # pragma: no cover
            self.key_t = self.key_t.__func__  # remove binding
        self._encode_prefixes()
        super().__init__(*args, **kwargs)
        if self.implements_incr:
            self.apply_chord = self._apply_chord_incr

    def _encode_prefixes(self) -> None:
        self.task_keyprefix = self.key_t(self.task_keyprefix)
        self.group_keyprefix = self.key_t(self.group_keyprefix)
        self.chord_keyprefix = self.key_t(self.chord_keyprefix)

    async def get(self, key: AnyStr) -> AnyStr:
        raise NotImplementedError('Must implement the get method.')

    async def mget(self, keys: Sequence[AnyStr]) -> Sequence[AnyStr]:
        raise NotImplementedError('Does not support get_many')

    async def set(self, key: AnyStr, value: AnyStr) -> None:
        raise NotImplementedError('Must implement the set method.')

    async def delete(self, key: AnyStr) -> None:
        raise NotImplementedError('Must implement the delete method')

    async def incr(self, key: AnyStr) -> None:
        raise NotImplementedError('Does not implement incr')

    async def expire(self, key: AnyStr, value: AnyStr) -> None:
        ...

    def get_key_for_task(self, task_id: str, key: AnyStr = '') -> AnyStr:
        """Get the cache key for a task by id."""
        key_t = self.key_t
        return key_t('').join([
            self.task_keyprefix, key_t(task_id), key_t(key),
        ])

    def get_key_for_group(self, group_id: str, key: AnyStr = '') -> AnyStr:
        """Get the cache key for a group by id."""
        key_t = self.key_t
        return key_t('').join([
            self.group_keyprefix, key_t(group_id), key_t(key),
        ])

    def get_key_for_chord(self, group_id: str, key: AnyStr = '') -> AnyStr:
        """Get the cache key for the chord waiting on group with given id."""
        key_t = self.key_t
        return key_t('').join([
            self.chord_keyprefix, key_t(group_id), key_t(key),
        ])

    def _strip_prefix(self, key: AnyStr) -> AnyStr:
        """Take bytes: emit string."""
        key = self.key_t(key)
        for prefix in self.task_keyprefix, self.group_keyprefix:
            if key.startswith(prefix):
                return bytes_to_str(key[len(prefix):])
        return bytes_to_str(key)

    def _filter_ready(
            self,
            values: Sequence[Tuple[Any, Any]],
            *,
            READY_STATES: Set[str] = states.READY_STATES) -> Iterable[Tuple]:
        for k, v in values:
            if v is not None:
                v = self.decode_result(v)
                if v['status'] in READY_STATES:
                    yield k, v

    def _mget_to_results(self, values: Any, keys: Sequence[AnyStr]) -> Mapping:
        if hasattr(values, 'items'):
            # client returns dict so mapping preserved.
            return {
                self._strip_prefix(k): v
                for k, v in self._filter_ready(values.items())
            }
        else:
            # client returns list so need to recreate mapping.
            return {
                bytes_to_str(keys[i]): v
                for i, v in self._filter_ready(enumerate(values))
            }

    async def get_many(
            self, task_ids: Sequence[str],
            *,
            timeout: float = None,
            interval: float = 0.5,
            no_ack: bool = True,
            on_message: Callable = None,
            on_interval: Callable = None,
            max_iterations: int = None,
            READY_STATES=states.READY_STATES) -> Iterator[str, Mapping]:
        interval = 0.5 if interval is None else interval
        ids = task_ids if isinstance(task_ids, set) else set(task_ids)
        cached_ids = set()
        cache = self._cache
        for task_id in ids:
            try:
                cached = cache[task_id]
            except KeyError:
                pass
            else:
                if cached['status'] in READY_STATES:
                    yield bytes_to_str(task_id), cached
                    cached_ids.add(task_id)

        ids.difference_update(cached_ids)
        iterations = 0
        while ids:
            keys = list(ids)
            payloads = await self.mget([
                self.get_key_for_task(k) for k in keys
            ])
            r = self._mget_to_results(payloads, keys)
            cache.update(r)
            ids.difference_update({bytes_to_str(v) for v in r})
            for key, value in r.items():
                if on_message is not None:
                    await on_message(value)
                yield bytes_to_str(key), value
            if timeout and iterations * interval >= timeout:
                raise TimeoutError('Operation timed out ({0})'.format(timeout))
            if on_interval:
                await on_interval()
            time.sleep(interval)  # don't busy loop.
            iterations += 1
            if max_iterations and iterations >= max_iterations:
                break

    async def _forget(self, task_id: str) -> None:
        await self.delete(self.get_key_for_task(task_id))

    async def _store_result(self, task_id: str, result: Any, state: str,
                            traceback: str = None,
                            request: RequestT = None,
                            **kwargs) -> Any:
        meta = {
            'status': state, 'result': result, 'traceback': traceback,
            'children': self.current_task_children(request),
            'task_id': bytes_to_str(task_id),
        }
        await self.set(self.get_key_for_task(task_id), self.encode(meta))
        return result

    async def _save_group(self,
                          group_id: str, result: GroupResult) -> GroupResult:
        await self.set(self.get_key_for_group(group_id),
                       self.encode({'result': result.as_tuple()}))
        return result

    async def _delete_group(self, group_id: str) -> None:
        await self.delete(self.get_key_for_group(group_id))

    async def _get_task_meta_for(self, task_id: str) -> Mapping:
        """Get task meta-data for a task by id."""
        meta = await self.get(self.get_key_for_task(task_id))
        if not meta:
            return {'status': states.PENDING, 'result': None}
        return self.decode_result(meta)

    async def _restore_group(self, group_id: str) -> GroupResult:
        """Get task meta-data for a task by id."""
        meta = await self.get(self.get_key_for_group(group_id))
        # previously this was always pickled, but later this
        # was extended to support other serializers, so the
        # structure is kind of weird.
        if meta:
            meta = self.decode(meta)
            result = meta['result']
            meta['result'] = result_from_tuple(result, self.app)
            return meta

    async def _apply_chord_incr(self, header: SignatureT,
                                partial_args: Sequence,
                                group_id: str,
                                body: SignatureT,
                                result: ResultT = None,
                                options: Mapping = {},
                                **kwargs) -> ResultT:
        self.ensure_chords_allowed()
        await self.save_group(group_id, self.app.GroupResult(group_id, result))

        fixed_options = {k: v for k, v in options.items() if k != 'task_id'}

        return await header(
            *partial_args, task_id=group_id, **fixed_options or {})

    async def on_chord_part_return(
            self,
            request: RequestT, state: str, result: Any,
            **kwargs) -> None:
        if not self.implements_incr:
            return
        app = self.app
        gid = request.group
        if not gid:
            return
        key = self.get_key_for_chord(gid)
        try:
            deps = await GroupResult.restore(gid, backend=self)
        except Exception as exc:  # pylint: disable=broad-except
            callback = maybe_signature(request.chord, app=app)
            logger.exception('Chord %r raised: %r', gid, exc)
            await self.chord_error_from_stack(
                callback,
                ChordError('Cannot restore group: {0!r}'.format(exc)),
            )
            return
        if deps is None:
            try:
                raise ValueError(gid)
            except ValueError as exc:
                callback = maybe_signature(request.chord, app=app)
                logger.exception('Chord callback %r raised: %r', gid, exc)
                await self.chord_error_from_stack(
                    callback,
                    ChordError('GroupResult {0} no longer exists'.format(gid)),
                )
                return
        val = await self.incr(key)
        size = len(deps)
        if val > size:  # pragma: no cover
            logger.warning('Chord counter incremented too many times for %r',
                           gid)
        elif val == size:
            callback = maybe_signature(request.chord, app=app)
            j = deps.join_native if deps.supports_native_join else deps.join
            try:
                with allow_join_result():
                    ret = await j(timeout=3.0, propagate=True)
            except Exception as exc:  # pylint: disable=broad-except
                try:
                    culprit = next(deps._failed_join_report())
                    reason = 'Dependency {0.id} raised {1!r}'.format(
                        culprit, exc,
                    )
                except StopIteration:
                    reason = repr(exc)

                logger.exception('Chord %r raised: %r', gid, reason)
                await self.chord_error_from_stack(callback, ChordError(reason))
            else:
                try:
                    await callback.delay(ret)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception('Chord %r raised: %r', gid, exc)
                    await self.chord_error_from_stack(
                        callback,
                        ChordError('Callback error: {0!r}'.format(exc)),
                    )
            finally:
                deps.delete()
                await self.client.delete(key)
        else:
            await self.expire(key, self.expires)


class KeyValueStoreBackend(BaseKeyValueStoreBackend, SyncBackendMixin):
    """Result backend base class for key/value stores."""


class DisabledBackend(BaseBackend):
    """Dummy result backend."""

    _cache = {}   # need this attribute to reset cache in tests.

    async def store_result(self, *args, **kwargs) -> Any:
        ...

    def ensure_chords_allowed(self) -> None:
        raise NotImplementedError(E_CHORD_NO_BACKEND.strip())

    def _is_disabled(self, *args, **kwargs) -> bool:
        raise NotImplementedError(E_NO_BACKEND.strip())

    def as_uri(self, *args, **kwargs) -> str:
        return 'disabled://'

    get_state = get_result = get_traceback = _is_disabled
    get_task_meta_for = wait_for = get_many = _is_disabled

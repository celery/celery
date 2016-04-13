# -*- coding: utf-8 -*-
"""
    celery.result
    ~~~~~~~~~~~~~

    Task results/state and groups of results.

"""
from __future__ import absolute_import, unicode_literals

import time

from collections import OrderedDict, deque
from contextlib import contextmanager
from copy import copy

from kombu.utils import cached_property
from vine import Thenable, barrier, promise

from . import current_app
from . import states
from ._state import _set_task_join_will_block, task_join_will_block
from .app import app_or_default
from .datastructures import DependencyGraph, GraphFormatter
from .exceptions import ImproperlyConfigured, IncompleteStream, TimeoutError
from .five import (
    items, python_2_unicode_compatible, range, string_t, monotonic,
)
from .utils import deprecated

try:
    import tblib
except ImportError:
    tblib = None

__all__ = [
    'ResultBase', 'AsyncResult', 'ResultSet',
    'GroupResult', 'EagerResult', 'result_from_tuple',
]

E_WOULDBLOCK = """\
Never call result.get() within a task!
See http://docs.celeryq.org/en/latest/userguide/tasks.html\
#task-synchronous-subtasks
"""


def assert_will_not_block():
    if task_join_will_block():
        raise RuntimeError(E_WOULDBLOCK)


@contextmanager
def allow_join_result():
    reset_value = task_join_will_block()
    _set_task_join_will_block(False)
    try:
        yield
    finally:
        _set_task_join_will_block(reset_value)


class ResultBase(object):
    """Base class for all results"""

    #: Parent result (if part of a chain)
    parent = None


@python_2_unicode_compatible
class AsyncResult(ResultBase):
    """Query task state.

    :param id: see :attr:`id`.
    :keyword backend: see :attr:`backend`.

    """
    app = None

    #: Error raised for timeouts.
    TimeoutError = TimeoutError

    #: The task's UUID.
    id = None

    #: The task result backend to use.
    backend = None

    def __init__(self, id, backend=None,
                 task_name=None,            # deprecated
                 app=None, parent=None):
        if id is None:
            raise ValueError(
                'AsyncResult requires valid id, not {0}'.format(type(id)))
        self.app = app_or_default(app or self.app)
        self.id = id
        self.backend = backend or self.app.backend
        self.parent = parent
        self.on_ready = promise(self._on_fulfilled)
        self._cache = None

    def then(self, callback, on_error=None):
        self.backend.add_pending_result(self)
        return self.on_ready.then(callback, on_error)

    def _on_fulfilled(self, result):
        self.backend.remove_pending_result(self)
        return result

    def as_tuple(self):
        parent = self.parent
        return (self.id, parent and parent.as_tuple()), None

    def forget(self):
        """Forget about (and possibly remove the result of) this task."""
        self._cache = None
        self.backend.forget(self.id)

    def revoke(self, connection=None, terminate=False, signal=None,
               wait=False, timeout=None):
        """Send revoke signal to all workers.

        Any worker receiving the task, or having reserved the
        task, *must* ignore it.

        :keyword terminate: Also terminate the process currently working
            on the task (if any).
        :keyword signal: Name of signal to send to process if terminate.
            Default is TERM.
        :keyword wait: Wait for replies from workers.  Will wait for 1 second
           by default or you can specify a custom ``timeout``.
        :keyword timeout: Time in seconds to wait for replies if ``wait``
                          enabled.

        """
        self.app.control.revoke(self.id, connection=connection,
                                terminate=terminate, signal=signal,
                                reply=wait, timeout=timeout)

    def get(self, timeout=None, propagate=True, interval=0.5,
            no_ack=True, follow_parents=True, callback=None, on_interval=None,
            EXCEPTION_STATES=states.EXCEPTION_STATES,
            PROPAGATE_STATES=states.PROPAGATE_STATES):
        """Wait until task is ready, and return its result.

        .. warning::

           Waiting for tasks within a task may lead to deadlocks.
           Please read :ref:`task-synchronous-subtasks`.

        :keyword timeout: How long to wait, in seconds, before the
                          operation times out.
        :keyword propagate: Re-raise exception if the task failed.
        :keyword interval: Time to wait (in seconds) before retrying to
           retrieve the result.  Note that this does not have any effect
           when using the amqp result store backend, as it does not
           use polling.
        :keyword no_ack: Enable amqp no ack (automatically acknowledge
            message).  If this is :const:`False` then the message will
            **not be acked**.
        :keyword follow_parents: Re-raise any exception raised by parent task.

        :raises celery.exceptions.TimeoutError: if `timeout` is not
            :const:`None` and the result does not arrive within `timeout`
            seconds.

        If the remote call raised an exception then that exception will
        be re-raised.

        """
        assert_will_not_block()
        _on_interval = promise()
        if follow_parents and propagate and self.parent:
            on_interval = promise(self._maybe_reraise_parent_error)
            self._maybe_reraise_parent_error()
        if on_interval:
            _on_interval.then(on_interval)

        if self._cache:
            if propagate:
                self.maybe_throw(callback=callback)
            return self.result

        self.backend.add_pending_result(self)
        return self.backend.wait_for_pending(
            self, timeout=timeout,
            interval=interval,
            on_interval=_on_interval,
            no_ack=no_ack,
            propagate=propagate,
            callback=callback,
        )
    wait = get  # deprecated alias to :meth:`get`.

    def _maybe_reraise_parent_error(self):
        for node in reversed(list(self._parents())):
            node.maybe_throw()

    def _parents(self):
        node = self.parent
        while node:
            yield node
            node = node.parent

    def collect(self, intermediate=False, **kwargs):
        """Iterator, like :meth:`get` will wait for the task to complete,
        but will also follow :class:`AsyncResult` and :class:`ResultSet`
        returned by the task, yielding ``(result, value)`` tuples for each
        result in the tree.

        An example would be having the following tasks:

        .. code-block:: python

            from celery import group
            from proj.celery import app

            @app.task(trail=True)
            def A(how_many):
                return group(B.s(i) for i in range(how_many))()

            @app.task(trail=True)
            def B(i):
                return pow2.delay(i)

            @app.task(trail=True)
            def pow2(i):
                return i ** 2

        Note that the ``trail`` option must be enabled
        so that the list of children is stored in ``result.children``.
        This is the default but enabled explicitly for illustration.

        Calling :meth:`collect` would return:

        .. code-block:: pycon

            >>> from celery.result import ResultBase
            >>> from proj.tasks import A

            >>> result = A.delay(10)
            >>> [v for v in result.collect()
            ...  if not isinstance(v, (ResultBase, tuple))]
            [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]

        """
        for _, R in self.iterdeps(intermediate=intermediate):
            yield R, R.get(**kwargs)

    def get_leaf(self):
        value = None
        for _, R in self.iterdeps():
            value = R.get()
        return value

    def iterdeps(self, intermediate=False):
        stack = deque([(None, self)])

        while stack:
            parent, node = stack.popleft()
            yield parent, node
            if node.ready():
                stack.extend((node, child) for child in node.children or [])
            else:
                if not intermediate:
                    raise IncompleteStream()

    def ready(self):
        """Returns :const:`True` if the task has been executed.

        If the task is still running, pending, or is waiting
        for retry then :const:`False` is returned.

        """
        return self.state in self.backend.READY_STATES

    def successful(self):
        """Returns :const:`True` if the task executed successfully."""
        return self.state == states.SUCCESS

    def failed(self):
        """Returns :const:`True` if the task failed."""
        return self.state == states.FAILURE

    def throw(self, *args, **kwargs):
        self.on_ready.throw(*args, **kwargs)

    def maybe_throw(self, propagate=True, callback=None):
        cache = self._get_task_meta() if self._cache is None else self._cache
        state, value, tb = (
            cache['status'], cache['result'], cache.get('traceback'))
        if state in states.PROPAGATE_STATES and propagate:
            self.throw(value, self._to_remote_traceback(tb))
        if callback is not None:
            callback(self.id, value)
        return value

    def _to_remote_traceback(self, tb):
        if tb and tblib is not None and self.app.conf.task_remote_tracebacks:
            return tblib.Traceback.from_string(tb).as_traceback()

    def build_graph(self, intermediate=False, formatter=None):
        graph = DependencyGraph(
            formatter=formatter or GraphFormatter(root=self.id, shape='oval'),
        )
        for parent, node in self.iterdeps(intermediate=intermediate):
            graph.add_arc(node)
            if parent:
                graph.add_edge(parent, node)
        return graph

    def __str__(self):
        """`str(self) -> self.id`"""
        return str(self.id)

    def __hash__(self):
        """`hash(self) -> hash(self.id)`"""
        return hash(self.id)

    def __repr__(self):
        return '<{0}: {1}>'.format(type(self).__name__, self.id)

    def __eq__(self, other):
        if isinstance(other, AsyncResult):
            return other.id == self.id
        elif isinstance(other, string_t):
            return other == self.id
        return NotImplemented

    def __ne__(self, other):
        res = self.__eq__(other)
        return True if res is NotImplemented else not res

    def __copy__(self):
        return self.__class__(
            self.id, self.backend, None, self.app, self.parent,
        )

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return self.id, self.backend, None, None, self.parent

    def __del__(self):
        self._cache = None

    @cached_property
    def graph(self):
        return self.build_graph()

    @property
    def supports_native_join(self):
        return self.backend.supports_native_join

    @property
    def children(self):
        return self._get_task_meta().get('children')

    def _maybe_set_cache(self, meta):
        if meta:
            state = meta['status']
            if state in states.READY_STATES:
                d = self._set_cache(self.backend.meta_from_decoded(meta))
                self.on_ready(self)
                return d
        return meta

    def _get_task_meta(self):
        if self._cache is None:
            return self._maybe_set_cache(self.backend.get_task_meta(self.id))
        return self._cache

    def _iter_meta(self):
        return iter([self._get_task_meta()])

    def _set_cache(self, d):
        children = d.get('children')
        if children:
            d['children'] = [
                result_from_tuple(child, self.app) for child in children
            ]
        self._cache = d
        return d

    @property
    def result(self):
        """When the task has been executed, this contains the return value.
        If the task raised an exception, this will be the exception
        instance."""
        return self._get_task_meta()['result']
    info = result

    @property
    def traceback(self):
        """Get the traceback of a failed task."""
        return self._get_task_meta().get('traceback')

    @property
    def state(self):
        """The tasks current state.

        Possible values includes:

            *PENDING*

                The task is waiting for execution.

            *STARTED*

                The task has been started.

            *RETRY*

                The task is to be retried, possibly because of failure.

            *FAILURE*

                The task raised an exception, or has exceeded the retry limit.
                The :attr:`result` attribute then contains the
                exception raised by the task.

            *SUCCESS*

                The task executed successfully. The :attr:`result` attribute
                then contains the tasks return value.

        """
        return self._get_task_meta()['status']
    status = state  # XXX compat

    @property
    def task_id(self):
        """compat alias to :attr:`id`"""
        return self.id

    @task_id.setter  # noqa
    def task_id(self, id):
        self.id = id
Thenable.register(AsyncResult)


@python_2_unicode_compatible
class ResultSet(ResultBase):
    """Working with more than one result.

    :param results: List of result instances.

    """
    _app = None

    #: List of results in in the set.
    results = None

    def __init__(self, results, app=None, ready_barrier=None, **kwargs):
        self._app = app
        self._cache = None
        self.results = results
        self.on_ready = promise(args=(self,))
        self._on_full = ready_barrier or barrier(results)
        if self._on_full:
            self._on_full.then(promise(self.on_ready))

    def add(self, result):
        """Add :class:`AsyncResult` as a new member of the set.

        Does nothing if the result is already a member.

        """
        if result not in self.results:
            self.results.append(result)
            if self._on_full:
                self._on_full.add(result)

    def _on_ready(self):
        self.backend.remove_pending_result(self)
        if self.backend.is_async:
            self._cache = [r.get() for r in self.results]
            self.on_ready()

    def remove(self, result):
        """Remove result from the set; it must be a member.

        :raises KeyError: if the result is not a member.

        """
        if isinstance(result, string_t):
            result = self.app.AsyncResult(result)
        try:
            self.results.remove(result)
        except ValueError:
            raise KeyError(result)

    def discard(self, result):
        """Remove result from the set if it is a member.

        If it is not a member, do nothing.

        """
        try:
            self.remove(result)
        except KeyError:
            pass

    def update(self, results):
        """Update set with the union of itself and an iterable with
        results."""
        self.results.extend(r for r in results if r not in self.results)

    def clear(self):
        """Remove all results from this set."""
        self.results[:] = []  # don't create new list.

    def successful(self):
        """Was all of the tasks successful?

        :returns: :const:`True` if all of the tasks finished
            successfully (i.e. did not raise an exception).

        """
        return all(result.successful() for result in self.results)

    def failed(self):
        """Did any of the tasks fail?

        :returns: :const:`True` if one of the tasks failed.
            (i.e., raised an exception)

        """
        return any(result.failed() for result in self.results)

    def maybe_throw(self, callback=None, propagate=True):
        for result in self.results:
            result.maybe_throw(callback=callback, propagate=propagate)

    def waiting(self):
        """Are any of the tasks incomplete?

        :returns: :const:`True` if one of the tasks are still
            waiting for execution.

        """
        return any(not result.ready() for result in self.results)

    def ready(self):
        """Did all of the tasks complete? (either by success of failure).

        :returns: :const:`True` if all of the tasks has been
            executed.

        """
        return all(result.ready() for result in self.results)

    def completed_count(self):
        """Task completion count.

        :returns: the number of tasks completed.

        """
        return sum(int(result.successful()) for result in self.results)

    def forget(self):
        """Forget about (and possible remove the result of) all the tasks."""
        for result in self.results:
            result.forget()

    def revoke(self, connection=None, terminate=False, signal=None,
               wait=False, timeout=None):
        """Send revoke signal to all workers for all tasks in the set.

        :keyword terminate: Also terminate the process currently working
            on the task (if any).
        :keyword signal: Name of signal to send to process if terminate.
            Default is TERM.
        :keyword wait: Wait for replies from worker.  Will wait for 1 second
           by default or you can specify a custom ``timeout``.
        :keyword timeout: Time in seconds to wait for replies if ``wait``
                          enabled.

        """
        self.app.control.revoke([r.id for r in self.results],
                                connection=connection, timeout=timeout,
                                terminate=terminate, signal=signal, reply=wait)

    def __iter__(self):
        return iter(self.results)

    def __getitem__(self, index):
        """`res[i] -> res.results[i]`"""
        return self.results[index]

    @deprecated('4.0', '5.0')
    def iterate(self, timeout=None, propagate=True, interval=0.5):
        """Deprecated method, use :meth:`get` with a callback argument."""
        elapsed = 0.0
        results = OrderedDict((result.id, copy(result))
                              for result in self.results)

        while results:
            removed = set()
            for task_id, result in items(results):
                if result.ready():
                    yield result.get(timeout=timeout and timeout - elapsed,
                                     propagate=propagate)
                    removed.add(task_id)
                else:
                    if result.backend.subpolling_interval:
                        time.sleep(result.backend.subpolling_interval)
            for task_id in removed:
                results.pop(task_id, None)
            time.sleep(interval)
            elapsed += interval
            if timeout and elapsed >= timeout:
                raise TimeoutError('The operation timed out')

    def get(self, timeout=None, propagate=True, interval=0.5,
            callback=None, no_ack=True, on_message=None):
        """See :meth:`join`

        This is here for API compatibility with :class:`AsyncResult`,
        in addition it uses :meth:`join_native` if available for the
        current result backend.

        """
        if self._cache is not None:
            return self._cache
        return (self.join_native if self.supports_native_join else self.join)(
            timeout=timeout, propagate=propagate,
            interval=interval, callback=callback, no_ack=no_ack,
            on_message=on_message,
        )

    def join(self, timeout=None, propagate=True, interval=0.5,
             callback=None, no_ack=True, on_message=None, on_interval=None):
        """Gathers the results of all tasks as a list in order.

        .. note::

            This can be an expensive operation for result store
            backends that must resort to polling (e.g. database).

            You should consider using :meth:`join_native` if your backend
            supports it.

        .. warning::

            Waiting for tasks within a task may lead to deadlocks.
            Please see :ref:`task-synchronous-subtasks`.

        :keyword timeout: The number of seconds to wait for results before
                          the operation times out.

        :keyword propagate: If any of the tasks raises an exception, the
                            exception will be re-raised.

        :keyword interval: Time to wait (in seconds) before retrying to
                           retrieve a result from the set.  Note that this
                           does not have any effect when using the amqp
                           result store backend, as it does not use polling.

        :keyword callback: Optional callback to be called for every result
                           received.  Must have signature ``(task_id, value)``
                           No results will be returned by this function if
                           a callback is specified.  The order of results
                           is also arbitrary when a callback is used.
                           To get access to the result object for a particular
                           id you will have to generate an index first:
                           ``index = {r.id: r for r in gres.results.values()}``
                           Or you can create new result objects on the fly:
                           ``result = app.AsyncResult(task_id)`` (both will
                           take advantage of the backend cache anyway).

        :keyword no_ack: Automatic message acknowledgment (Note that if this
            is set to :const:`False` then the messages *will not be
            acknowledged*).

        :raises celery.exceptions.TimeoutError: if ``timeout`` is not
            :const:`None` and the operation takes longer than ``timeout``
            seconds.

        """
        assert_will_not_block()
        time_start = monotonic()
        remaining = None

        if on_message is not None:
            raise ImproperlyConfigured(
                'Backend does not support on_message callback')

        results = []
        for result in self.results:
            remaining = None
            if timeout:
                remaining = timeout - (monotonic() - time_start)
                if remaining <= 0.0:
                    raise TimeoutError('join operation timed out')
            value = result.get(
                timeout=remaining, propagate=propagate,
                interval=interval, no_ack=no_ack, on_interval=on_interval,
            )
            if callback:
                callback(result.id, value)
            else:
                results.append(value)
        return results

    def then(self, callback, on_error=None):
        return self.on_ready.then(callback, on_error)

    def iter_native(self, timeout=None, interval=0.5, no_ack=True,
                    on_message=None, on_interval=None):
        """Backend optimized version of :meth:`iterate`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the amqp, Redis and cache
        result backends.

        """
        return self.backend.iter_native(
            self,
            timeout=timeout, interval=interval, no_ack=no_ack,
            on_message=on_message, on_interval=on_interval,
        )

    def join_native(self, timeout=None, propagate=True,
                    interval=0.5, callback=None, no_ack=True,
                    on_message=None, on_interval=None):
        """Backend optimized version of :meth:`join`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the amqp, Redis and cache
        result backends.

        """
        assert_will_not_block()
        order_index = None if callback else {
            result.id: i for i, result in enumerate(self.results)
        }
        acc = None if callback else [None for _ in range(len(self))]
        for task_id, meta in self.iter_native(timeout, interval, no_ack,
                                              on_message, on_interval):
            value = meta['result']
            if propagate and meta['status'] in states.PROPAGATE_STATES:
                raise value
            if callback:
                callback(task_id, value)
            else:
                acc[order_index[task_id]] = value
        return acc

    def _iter_meta(self):
        return (meta for _, meta in self.backend.get_many(
            {r.id for r in self.results}, max_iterations=1,
        ))

    def _failed_join_report(self):
        return (res for res in self.results
                if res.backend.is_cached(res.id) and
                res.state in states.PROPAGATE_STATES)

    def __len__(self):
        return len(self.results)

    def __eq__(self, other):
        if isinstance(other, ResultSet):
            return other.results == self.results
        return NotImplemented

    def __ne__(self, other):
        res = self.__eq__(other)
        return True if res is NotImplemented else not res

    def __repr__(self):
        return '<{0}: [{1}]>'.format(type(self).__name__,
                                     ', '.join(r.id for r in self.results))

    @property
    def supports_native_join(self):
        try:
            return self.results[0].supports_native_join
        except IndexError:
            pass

    @property
    def app(self):
        if self._app is None:
            self._app = (self.results[0].app if self.results else
                         current_app._get_current_object())
        return self._app

    @app.setter
    def app(self, app):  # noqa
        self._app = app

    @property
    def backend(self):
        return self.app.backend if self.app else self.results[0].backend
Thenable.register(ResultSet)


@python_2_unicode_compatible
class GroupResult(ResultSet):
    """Like :class:`ResultSet`, but with an associated id.

    This type is returned by :class:`~celery.group`.

    It enables inspection of the tasks state and return values as
    a single entity.

    :param id: The id of the group.
    :param results: List of result instances.

    """

    #: The UUID of the group.
    id = None

    #: List/iterator of results in the group
    results = None

    def __init__(self, id=None, results=None, **kwargs):
        self.id = id
        ResultSet.__init__(self, results, **kwargs)

    def save(self, backend=None):
        """Save group-result for later retrieval using :meth:`restore`.

        Example::

            >>> def save_and_restore(result):
            ...     result.save()
            ...     result = GroupResult.restore(result.id)

        """
        return (backend or self.app.backend).save_group(self.id, self)

    def delete(self, backend=None):
        """Remove this result if it was previously saved."""
        (backend or self.app.backend).delete_group(self.id)

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return self.id, self.results

    def __bool__(self):
        return bool(self.id or self.results)
    __nonzero__ = __bool__  # Included for Py2 backwards compatibility

    def __eq__(self, other):
        if isinstance(other, GroupResult):
            return other.id == self.id and other.results == self.results
        return NotImplemented

    def __ne__(self, other):
        res = self.__eq__(other)
        return True if res is NotImplemented else not res

    def __repr__(self):
        return '<{0}: {1} [{2}]>'.format(type(self).__name__, self.id,
                                         ', '.join(r.id for r in self.results))

    def as_tuple(self):
        return self.id, [r.as_tuple() for r in self.results]

    @property
    def children(self):
        return self.results

    @classmethod
    def restore(self, id, backend=None):
        """Restore previously saved group result."""
        return (
            backend or (self.app.backend if self.app else current_app.backend)
        ).restore_group(id)
Thenable.register(ResultSet)


@python_2_unicode_compatible
class EagerResult(AsyncResult):
    """Result that we know has already been executed."""

    def __init__(self, id, ret_value, state, traceback=None):
        self.id = id
        self._result = ret_value
        self._state = state
        self._traceback = traceback
        self.on_ready = promise(args=(self,))
        self.on_ready()

    def then(self, callback, on_error=None):
        return self.on_ready.then(callback, on_error)

    def _get_task_meta(self):
        return self._cache

    def __del__(self):
        pass

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return (self.id, self._result, self._state, self._traceback)

    def __copy__(self):
        cls, args = self.__reduce__()
        return cls(*args)

    def ready(self):
        return True

    def get(self, timeout=None, propagate=True, **kwargs):
        if self.successful():
            return self.result
        elif self.state in states.PROPAGATE_STATES:
            if propagate:
                raise self.result
            return self.result
    wait = get  # XXX Compat (remove 5.0)

    def forget(self):
        pass

    def revoke(self, *args, **kwargs):
        self._state = states.REVOKED

    def __repr__(self):
        return '<EagerResult: {0.id}>'.format(self)

    @property
    def _cache(self):
        return {'task_id': self.id, 'result': self._result, 'status':
                self._state, 'traceback': self._traceback}

    @property
    def result(self):
        """The tasks return value"""
        return self._result

    @property
    def state(self):
        """The tasks state."""
        return self._state
    status = state

    @property
    def traceback(self):
        """The traceback if the task failed."""
        return self._traceback

    @property
    def supports_native_join(self):
        return False
Thenable.register(EagerResult)


def result_from_tuple(r, app=None):
    # earlier backends may just pickle, so check if
    # result is already prepared.
    app = app_or_default(app)
    Result = app.AsyncResult
    if not isinstance(r, ResultBase):
        res, nodes = r
        if nodes:
            return app.GroupResult(
                res, [result_from_tuple(child, app) for child in nodes],
            )
        # previously did not include parent
        id, parent = res if isinstance(res, (list, tuple)) else (res, None)
        if parent:
            parent = result_from_tuple(parent, app)
        return Result(id, parent=parent)
    return r

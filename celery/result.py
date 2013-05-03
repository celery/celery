# -*- coding: utf-8 -*-
"""
    celery.result
    ~~~~~~~~~~~~~

    Task results/state and groups of results.

"""
from __future__ import absolute_import
from __future__ import with_statement

import time

from collections import deque
from copy import copy

from kombu.utils import cached_property
from kombu.utils.compat import OrderedDict

from . import current_app
from . import states
from .app import app_or_default
from .datastructures import DependencyGraph
from .exceptions import IncompleteStream, TimeoutError


class ResultBase(object):
    """Base class for all results"""


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

    #: Parent result (if part of a chain)
    parent = None

    def __init__(self, id, backend=None, task_name=None,
                 app=None, parent=None):
        self.app = app_or_default(app or self.app)
        self.id = id
        self.backend = backend or self.app.backend
        self.task_name = task_name
        self.parent = parent

    def serializable(self):
        return self.id, None

    def forget(self):
        """Forget about (and possibly remove the result of) this task."""
        self.backend.forget(self.id)

    def revoke(self, connection=None, terminate=False, signal=None):
        """Send revoke signal to all workers.

        Any worker receiving the task, or having reserved the
        task, *must* ignore it.

        :keyword terminate: Also terminate the process currently working
            on the task (if any).
        :keyword signal: Name of signal to send to process if terminate.
            Default is TERM.

        """
        self.app.control.revoke(self.id, connection=connection,
                                terminate=terminate, signal=signal)

    def get(self, timeout=None, propagate=True, interval=0.5):
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

        :raises celery.exceptions.TimeoutError: if `timeout` is not
            :const:`None` and the result does not arrive within `timeout`
            seconds.

        If the remote call raised an exception then that exception will
        be re-raised.

        """
        return self.backend.wait_for(self.id, timeout=timeout,
                                     propagate=propagate,
                                     interval=interval)
    wait = get  # deprecated alias to :meth:`get`.

    def collect(self, intermediate=False, **kwargs):
        """Iterator, like :meth:`get` will wait for the task to complete,
        but will also follow :class:`AsyncResult` and :class:`ResultSet`
        returned by the task, yielding for each result in the tree.

        An example would be having the following tasks:

        .. code-block:: python

            @task()
            def A(how_many):
                return group(B.s(i) for i in xrange(how_many))

            @task()
            def B(i):
                return pow2.delay(i)

            @task()
            def pow2(i):
                return i ** 2

        Calling :meth:`collect` would return:

        .. code-block:: python

            >>> result = A.delay(10)
            >>> list(result.collect())
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

    def build_graph(self, intermediate=False):
        graph = DependencyGraph()
        for parent, node in self.iterdeps(intermediate=intermediate):
            if parent:
                graph.add_arc(parent)
                graph.add_edge(parent, node)
        return graph

    def __str__(self):
        """`str(self) -> self.id`"""
        return str(self.id)

    def __hash__(self):
        """`hash(self) -> hash(self.id)`"""
        return hash(self.id)

    def __repr__(self):
        return '<%s: %s>' % (self.__class__.__name__, self.id)

    def __eq__(self, other):
        if isinstance(other, AsyncResult):
            return other.id == self.id
        elif isinstance(other, basestring):
            return other == self.id
        return NotImplemented

    def __copy__(self):
        r = self.__reduce__()
        return r[0](*r[1])

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return self.id, self.backend, self.task_name, None, self.parent

    @cached_property
    def graph(self):
        return self.build_graph()

    @property
    def supports_native_join(self):
        return self.backend.supports_native_join

    @property
    def children(self):
        children = self.backend.get_children(self.id)
        if children:
            return [from_serializable(r, self.app) for r in children]

    @property
    def result(self):
        """When the task has been executed, this contains the return value.
        If the task raised an exception, this will be the exception
        instance."""
        return self.backend.get_result(self.id)
    info = result

    @property
    def traceback(self):
        """Get the traceback of a failed task."""
        return self.backend.get_traceback(self.id)

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
        return self.backend.get_status(self.id)
    status = state

    def _get_task_id(self):
        return self.id

    def _set_task_id(self, id):
        self.id = id
    task_id = property(_get_task_id, _set_task_id)
BaseAsyncResult = AsyncResult  # for backwards compatibility.


class ResultSet(ResultBase):
    """Working with more than one result.

    :param results: List of result instances.

    """
    app = None

    #: List of results in in the set.
    results = None

    def __init__(self, results, app=None, **kwargs):
        self.app = app_or_default(app or self.app)
        self.results = results

    def add(self, result):
        """Add :class:`AsyncResult` as a new member of the set.

        Does nothing if the result is already a member.

        """
        if result not in self.results:
            self.results.append(result)

    def remove(self, result):
        """Removes result from the set; it must be a member.

        :raises KeyError: if the result is not a member.

        """
        if isinstance(result, basestring):
            result = AsyncResult(result)
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

    def revoke(self, connection=None, terminate=False, signal=None):
        """Send revoke signal to all workers for all tasks in the set.

        :keyword terminate: Also terminate the process currently working
            on the task (if any).
        :keyword signal: Name of signal to send to process if terminate.
            Default is TERM.

        """
        with self.app.connection_or_acquire(connection) as conn:
            for result in self.results:
                result.revoke(
                    connection=conn, terminate=terminate, signal=signal,
                )

    def __iter__(self):
        return self.iterate()

    def __getitem__(self, index):
        """`res[i] -> res.results[i]`"""
        return self.results[index]

    def iterate(self, timeout=None, propagate=True, interval=0.5):
        """Iterate over the return values of the tasks as they finish
        one by one.

        :raises: The exception if any of the tasks raised an exception.

        """
        elapsed = 0.0
        results = OrderedDict((result.id, copy(result))
                              for result in self.results)

        while results:
            removed = set()
            for task_id, result in results.iteritems():
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

    def get(self, timeout=None, propagate=True, interval=0.5):
        """See :meth:`join`

        This is here for API compatibility with :class:`AsyncResult`,
        in addition it uses :meth:`join_native` if available for the
        current result backend.

        """
        return (self.join_native if self.supports_native_join else self.join)(
            timeout=timeout, propagate=propagate, interval=interval)

    def join(self, timeout=None, propagate=True, interval=0.5):
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

        :raises celery.exceptions.TimeoutError: if `timeout` is not
            :const:`None` and the operation takes longer than `timeout`
            seconds.

        """
        time_start = time.time()
        remaining = None

        results = []
        for result in self.results:
            remaining = None
            if timeout:
                remaining = timeout - (time.time() - time_start)
                if remaining <= 0.0:
                    raise TimeoutError('join operation timed out')
            results.append(result.get(timeout=remaining,
                                      propagate=propagate,
                                      interval=interval))
        return results

    def iter_native(self, timeout=None, interval=None):
        """Backend optimized version of :meth:`iterate`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the amqp, Redis and cache
        result backends.

        """
        if not self.results:
            return iter([])
        backend = self.results[0].backend
        ids = [result.id for result in self.results]
        return backend.get_many(ids, timeout=timeout, interval=interval)

    def join_native(self, timeout=None, propagate=True, interval=0.5):
        """Backend optimized version of :meth:`join`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the amqp, Redis and cache
        result backends.

        """
        results = self.results
        acc = [None for _ in xrange(len(self))]
        for task_id, meta in self.iter_native(timeout=timeout,
                                              interval=interval):
            if propagate and meta['status'] in states.PROPAGATE_STATES:
                raise meta['result']
            acc[results.index(task_id)] = meta['result']
        return acc

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

    def __repr__(self):
        return '<%s: [%s]>' % (self.__class__.__name__,
                               ', '.join(r.id for r in self.results))

    @property
    def subtasks(self):
        """Deprecated alias to :attr:`results`."""
        return self.results

    @property
    def supports_native_join(self):
        return self.results[0].supports_native_join


class GroupResult(ResultSet):
    """Like :class:`ResultSet`, but with an associated id.

    This type is returned by :class:`~celery.group`, and the
    deprecated TaskSet, meth:`~celery.task.TaskSet.apply_async` method.

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

            >>> result.save()
            >>> result = GroupResult.restore(group_id)

        """
        return (backend or self.app.backend).save_group(self.id, self)

    def delete(self, backend=None):
        """Remove this result if it was previously saved."""
        (backend or self.app.backend).delete_group(self.id)

    def __reduce__(self):
        return self.__class__, self.__reduce_args__()

    def __reduce_args__(self):
        return self.id, self.results

    def __eq__(self, other):
        if isinstance(other, GroupResult):
            return other.id == self.id and other.results == self.results
        return NotImplemented

    def __repr__(self):
        return '<%s: %s [%s]>' % (self.__class__.__name__, self.id,
                                  ', '.join(r.id for r in self.results))

    def serializable(self):
        return self.id, [r.serializable() for r in self.results]

    @property
    def children(self):
        return self.results

    @classmethod
    def restore(self, id, backend=None):
        """Restore previously saved group result."""
        return (backend or current_app.backend).restore_group(id)


class TaskSetResult(GroupResult):
    """Deprecated version of :class:`GroupResult`"""

    def __init__(self, taskset_id, results=None, **kwargs):
        # XXX supports the taskset_id kwarg.
        # XXX previously the "results" arg was named "subtasks".
        if 'subtasks' in kwargs:
            results = kwargs['subtasks']
        GroupResult.__init__(self, taskset_id, results, **kwargs)

    def itersubtasks(self):
        """Deprecated.   Use ``iter(self.results)`` instead."""
        return iter(self.results)

    @property
    def total(self):
        """Deprecated: Use ``len(r)``."""
        return len(self)

    def _get_taskset_id(self):
        return self.id

    def _set_taskset_id(self, id):
        self.id = id
    taskset_id = property(_get_taskset_id, _set_taskset_id)


class EagerResult(AsyncResult):
    """Result that we know has already been executed."""
    task_name = None

    def __init__(self, id, ret_value, state, traceback=None):
        self.id = id
        self._result = ret_value
        self._state = state
        self._traceback = traceback

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
    wait = get

    def forget(self):
        pass

    def revoke(self, *args, **kwargs):
        self._state = states.REVOKED

    def __repr__(self):
        return '<EagerResult: %s>' % self.id

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


def from_serializable(r, app=None):
    # earlier backends may just pickle, so check if
    # result is already prepared.
    app = app_or_default(app)
    Result = app.AsyncResult
    if not isinstance(r, ResultBase):
        if isinstance(r, (list, tuple)):
            id, nodes = r
            if nodes:
                return app.GroupResult(id, [Result(sid) for sid, _ in nodes])
            return Result(id)
        else:
            return Result(r)
    return r

# -*- coding: utf-8 -*-
"""
    celery.result
    ~~~~~~~~~~~~~

    Task results/state and groups of results.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import
from __future__ import with_statement

import time

from copy import copy
from itertools import imap

from . import current_app
from . import states
from .app import app_or_default
from .exceptions import TimeoutError
from .registry import _unpickle_task
from .utils.compat import OrderedDict


def _unpickle_result(task_id, task_name):
    return _unpickle_task(task_name).AsyncResult(task_id)


class BaseAsyncResult(object):
    """Base class for pending result, supports custom task result backend.

    :param task_id: see :attr:`task_id`.
    :param backend: see :attr:`backend`.

    """

    #: Error raised for timeouts.
    TimeoutError = TimeoutError

    #: The task uuid.
    task_id = None

    #: The task result backend to use.
    backend = None

    def __init__(self, task_id, backend, task_name=None, app=None):
        self.app = app_or_default(app)
        self.task_id = task_id
        self.backend = backend
        self.task_name = task_name

    def forget(self):
        """Forget about (and possibly remove the result of) this task."""
        self.backend.forget(self.task_id)

    def revoke(self, connection=None, connect_timeout=None):
        """Send revoke signal to all workers.

        Any worker receiving the task, or having reserved the
        task, *must* ignore it.

        """
        self.app.control.revoke(self.task_id, connection=connection,
                                connect_timeout=connect_timeout)

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
           when using the AMQP result store backend, as it does not
           use polling.

        :raises celery.exceptions.TimeoutError: if `timeout` is not
            :const:`None` and the result does not arrive within `timeout`
            seconds.

        If the remote call raised an exception then that exception will
        be re-raised.

        """
        return self.backend.wait_for(self.task_id, timeout=timeout,
                                                   propagate=propagate,
                                                   interval=interval)

    def wait(self, *args, **kwargs):
        """Deprecated alias to :meth:`get`."""
        return self.get(*args, **kwargs)

    def ready(self):
        """Returns :const:`True` if the task has been executed.

        If the task is still running, pending, or is waiting
        for retry then :const:`False` is returned.

        """
        return self.status in self.backend.READY_STATES

    def successful(self):
        """Returns :const:`True` if the task executed successfully."""
        return self.status == states.SUCCESS

    def failed(self):
        """Returns :const:`True` if the task failed."""
        return self.status == states.FAILURE

    def __str__(self):
        """`str(self) -> self.task_id`"""
        return self.task_id

    def __hash__(self):
        """`hash(self) -> hash(self.task_id)`"""
        return hash(self.task_id)

    def __repr__(self):
        return "<AsyncResult: %s>" % self.task_id

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.task_id == other.task_id
        return other == self.task_id

    def __copy__(self):
        return self.__class__(self.task_id, backend=self.backend)

    def __reduce__(self):
        if self.task_name:
            return (_unpickle_result, (self.task_id, self.task_name))
        else:
            return (self.__class__, (self.task_id, self.backend,
                                     None, self.app))

    @property
    def result(self):
        """When the task has been executed, this contains the return value.
        If the task raised an exception, this will be the exception
        instance."""
        return self.backend.get_result(self.task_id)

    @property
    def info(self):
        """Get state metadata.  Alias to :meth:`result`."""
        return self.result

    @property
    def traceback(self):
        """Get the traceback of a failed task."""
        return self.backend.get_traceback(self.task_id)

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
        return self.backend.get_status(self.task_id)

    @property
    def status(self):
        """Deprecated alias of :attr:`state`."""
        return self.state


class AsyncResult(BaseAsyncResult):
    """Pending task result using the default backend.

    :param task_id: The task uuid.

    """

    #: Task result store backend to use.
    backend = None

    def __init__(self, task_id, backend=None, task_name=None, app=None):
        app = app_or_default(app)
        backend = backend or app.backend
        super(AsyncResult, self).__init__(task_id, backend,
                                          task_name=task_name, app=app)


class ResultSet(object):
    """Working with more than one result.

    :param results: List of result instances.

    """

    #: List of results in in the set.
    results = None

    def __init__(self, results, app=None, **kwargs):
        self.app = app_or_default(app)
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

        :returns: :const:`True` if any of the tasks failed.
            (i.e., raised an exception)

        """
        return any(result.failed() for result in self.results)

    def waiting(self):
        """Are any of the tasks incomplete?

        :returns: :const:`True` if any of the tasks is still
            waiting for execution.

        """
        return any(not result.ready() for result in self.results)

    def ready(self):
        """Did all of the tasks complete? (either by success of failure).

        :returns: :const:`True` if all of the tasks been
            executed.

        """
        return all(result.ready() for result in self.results)

    def completed_count(self):
        """Task completion count.

        :returns: the number of tasks completed.

        """
        return sum(imap(int, (result.successful() for result in self.results)))

    def forget(self):
        """Forget about (and possible remove the result of) all the tasks."""
        for result in self.results:
            result.forget()

    def revoke(self, connection=None, connect_timeout=None):
        """Revoke all tasks in the set."""
        with self.app.default_connection(connection, connect_timeout) as conn:
            for result in self.results:
                result.revoke(connection=conn)

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
        results = OrderedDict((result.task_id, copy(result))
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
                raise TimeoutError("The operation timed out")

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
                           does not have any effect when using the AMQP
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
                    raise TimeoutError("join operation timed out")
            results.append(result.wait(timeout=remaining,
                                       propagate=propagate,
                                       interval=interval))
        return results

    def iter_native(self, timeout=None, interval=None):
        """Backend optimized version of :meth:`iterate`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the AMQP, Redis and cache
        result backends.

        """
        backend = self.results[0].backend
        ids = [result.task_id for result in self.results]
        return backend.get_many(ids, timeout=timeout, interval=interval)

    def join_native(self, timeout=None, propagate=True, interval=0.5):
        """Backend optimized version of :meth:`join`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the AMQP, Redis and cache
        result backends.

        """
        results = self.results
        acc = [None for _ in xrange(self.total)]
        for task_id, meta in self.iter_native(timeout=timeout,
                                              interval=interval):
            acc[results.index(task_id)] = meta["result"]
        return acc

    @property
    def total(self):
        """Total number of tasks in the set."""
        return len(self.results)

    @property
    def subtasks(self):
        """Deprecated alias to :attr:`results`."""
        return self.results


class TaskSetResult(ResultSet):
    """An instance of this class is returned by
    `TaskSet`'s :meth:`~celery.task.TaskSet.apply_async` method.

    It enables inspection of the tasks state and return values as
    a single entity.

    :param taskset_id: The id of the taskset.
    :param results: List of result instances.

    """

    #: The UUID of the taskset.
    taskset_id = None

    #: List/iterator of results in the taskset
    results = None

    def __init__(self, taskset_id, results=None, **kwargs):
        self.taskset_id = taskset_id

        # XXX previously the "results" arg was named "subtasks".
        if "subtasks" in kwargs:
            results = kwargs["subtasks"]
        super(TaskSetResult, self).__init__(results, **kwargs)

    def save(self, backend=None):
        """Save taskset result for later retrieval using :meth:`restore`.

        Example::

            >>> result.save()
            >>> result = TaskSetResult.restore(taskset_id)

        """
        return (backend or self.app.backend).save_taskset(self.taskset_id,
                                                          self)

    def delete(self, backend=None):
        """Remove this result if it was previously saved."""
        (backend or self.app.backend).delete_taskset(self.taskset_id)

    @classmethod
    def restore(self, taskset_id, backend=None):
        """Restore previously saved taskset result."""
        return (backend or current_app.backend).restore_taskset(taskset_id)

    def itersubtasks(self):
        """Depreacted.   Use ``iter(self.results)`` instead."""
        return iter(self.results)

    def __reduce__(self):
        return (self.__class__, (self.taskset_id, self.results))


class EagerResult(BaseAsyncResult):
    """Result that we know has already been executed."""
    TimeoutError = TimeoutError

    def __init__(self, task_id, ret_value, state, traceback=None):
        self.task_id = task_id
        self._result = ret_value
        self._state = state
        self._traceback = traceback

    def __reduce__(self):
        return (self.__class__, (self.task_id, self._result,
                                 self._state, self._traceback))

    def __copy__(self):
        cls, args = self.__reduce__()
        return cls(*args)

    def successful(self):
        """Returns :const:`True` if the task executed without failure."""
        return self.state == states.SUCCESS

    def ready(self):
        """Returns :const:`True` if the task has been executed."""
        return True

    def get(self, timeout=None, propagate=True, **kwargs):
        """Wait until the task has been executed and return its result."""
        if self.state == states.SUCCESS:
            return self.result
        elif self.state in states.PROPAGATE_STATES:
            if propagate:
                raise self.result
            return self.result

    def revoke(self):
        self._state = states.REVOKED

    def __repr__(self):
        return "<EagerResult: %s>" % self.task_id

    @property
    def result(self):
        """The tasks return value"""
        return self._result

    @property
    def state(self):
        """The tasks state."""
        return self._state

    @property
    def traceback(self):
        """The traceback if the task failed."""
        return self._traceback

    @property
    def status(self):
        """The tasks status (alias to :attr:`state`)."""
        return self._state

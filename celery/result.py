from __future__ import generators

import time

from copy import copy
from itertools import imap

from celery import current_app
from celery import states
from celery.app import app_or_default
from celery.exceptions import TimeoutError
from celery.registry import _unpickle_task
from celery.utils.compat import any, all


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
        self.task_id = task_id
        self.backend = backend
        self.task_name = task_name
        self.app = app_or_default(app)

    def __reduce__(self):
        if self.task_name:
            return (_unpickle_result, (self.task_id, self.task_name))
        else:
            return (self.__class__, (self.task_id, self.backend,
                                     None, self.app))

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

           Waiting for subtasks may lead to deadlocks.
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
        """Returns :const:`True` if the task failed by exception."""
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
    def status(self):
        """Deprecated alias of :attr:`state`."""
        return self.state

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

                The task raised an exception, or has been retried more times
                than its limit. The :attr:`result` attribute contains the
                exception raised.

            *SUCCESS*

                The task executed successfully. The :attr:`result` attribute
                contains the resulting value.

        """
        return self.backend.get_status(self.task_id)


class AsyncResult(BaseAsyncResult):
    """Pending task result using the default backend.

    :param task_id: The tasks uuid.

    """

    #: Task result store backend to use.
    backend = None

    def __init__(self, task_id, backend=None, task_name=None, app=None):
        app = app_or_default(app)
        backend = backend or app.backend
        super(AsyncResult, self).__init__(task_id, backend,
                                          task_name=task_name, app=app)


class TaskSetResult(object):
    """Working with :class:`~celery.task.sets.TaskSet` results.

    An instance of this class is returned by
    `TaskSet`'s :meth:`~celery.task.TaskSet.apply_async()`.  It enables
    inspection of the subtasks state and return values as a single entity.

    :param taskset_id: The id of the taskset.
    :param subtasks: List of result instances.

    """

    #: The UUID of the taskset.
    taskset_id = None

    #: A list of :class:`AsyncResult` instances for all of the subtasks.
    subtasks = None

    def __init__(self, taskset_id, subtasks, app=None):
        self.taskset_id = taskset_id
        self.subtasks = subtasks
        self.app = app_or_default(app)

    def itersubtasks(self):
        """Taskset subtask iterator.

        :returns: an iterator for iterating over the tasksets
            :class:`AsyncResult` objects.

        """
        return (subtask for subtask in self.subtasks)

    def successful(self):
        """Was the taskset successful?

        :returns: :const:`True` if all of the tasks in the taskset finished
            successfully (i.e. did not raise an exception).

        """
        return all(subtask.successful()
                        for subtask in self.itersubtasks())

    def failed(self):
        """Did the taskset fail?

        :returns: :const:`True` if any of the tasks in the taskset failed.
            (i.e., raised an exception)

        """
        return any(subtask.failed()
                        for subtask in self.itersubtasks())

    def waiting(self):
        """Is the taskset waiting?

        :returns: :const:`True` if any of the tasks in the taskset is still
            waiting for execution.

        """
        return any(not subtask.ready()
                        for subtask in self.itersubtasks())

    def ready(self):
        """Is the task ready?

        :returns: :const:`True` if all of the tasks in the taskset has been
            executed.

        """
        return all(subtask.ready()
                        for subtask in self.itersubtasks())

    def completed_count(self):
        """Task completion count.

        :returns: the number of tasks completed.

        """
        return sum(imap(int, (subtask.successful()
                                for subtask in self.itersubtasks())))

    def forget(self):
        """Forget about (and possible remove the result of) all the tasks
        in this taskset."""
        for subtask in self.subtasks:
            subtask.forget()

    def revoke(self, connection=None, connect_timeout=None):
        """Revoke all subtasks."""

        def _do_revoke(connection=None, connect_timeout=None):
            for subtask in self.subtasks:
                subtask.revoke(connection=connection)

        return self.app.with_default_connection(_do_revoke)(
                connection=connection, connect_timeout=connect_timeout)

    def __iter__(self):
        """`iter(res)` -> `res.iterate()`."""
        return self.iterate()

    def __getitem__(self, index):
        """`res[i] -> res.subtasks[i]`"""
        return self.subtasks[index]

    def iterate(self):
        """Iterate over the return values of the tasks as they finish
        one by one.

        :raises: The exception if any of the tasks raised an exception.

        """
        pending = list(self.subtasks)
        results = dict((subtask.task_id, copy(subtask))
                            for subtask in self.subtasks)
        while pending:
            for task_id in pending:
                result = results[task_id]
                if result.status == states.SUCCESS:
                    try:
                        pending.remove(task_id)
                    except ValueError:
                        pass
                    yield result.result
                elif result.status in states.PROPAGATE_STATES:
                    raise result.result

    def join(self, timeout=None, propagate=True, interval=0.5):
        """Gathers the results of all tasks in the taskset,
        and returns a list ordered by the order of the set.

        .. note::

            This can be an expensive operation for result store
            backends that must resort to polling (e.g. database).

            You should consider using :meth:`join_native` if your backend
            supports it.

        .. warning::

            Waiting for subtasks may lead to deadlocks.
            Please see :ref:`task-synchronous-subtasks`.

        :keyword timeout: The number of seconds to wait for results before
                          the operation times out.

        :keyword propagate: If any of the subtasks raises an exception, the
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
        for subtask in self.subtasks:
            remaining = None
            if timeout:
                remaining = timeout - (time.time() - time_start)
                if remaining <= 0.0:
                    raise TimeoutError("join operation timed out")
            results.append(subtask.wait(timeout=remaining,
                                        propagate=propagate,
                                        interval=interval))
        return results

    def iter_native(self, timeout=None):
        backend = self.subtasks[0].backend
        ids = [subtask.task_id for subtask in self.subtasks]
        return backend.get_many(ids, timeout=timeout)

    def join_native(self, timeout=None, propagate=True):
        """Backend optimized version of :meth:`join`.

        .. versionadded:: 2.2

        Note that this does not support collecting the results
        for different task types using different backends.

        This is currently only supported by the AMQP result backend.

        """
        backend = self.subtasks[0].backend
        results = [None for _ in xrange(len(self.subtasks))]

        ids = [subtask.task_id for subtask in self.subtasks]
        states = dict(backend.get_many(ids, timeout=timeout))

        for task_id, meta in states.items():
            index = self.subtasks.index(task_id)
            results[index] = meta["result"]

        return list(results)

    def save(self, backend=None):
        """Save taskset result for later retrieval using :meth:`restore`.

        Example::

            >>> result.save()
            >>> result = TaskSetResult.restore(taskset_id)

        """
        if backend is None:
            backend = self.app.backend
        backend.save_taskset(self.taskset_id, self)

    @classmethod
    def restore(self, taskset_id, backend=None):
        """Restore previously saved taskset result."""
        if backend is None:
            backend = current_app.backend
        return backend.restore_taskset(taskset_id)

    @property
    def total(self):
        """Total number of subtasks in the set."""
        return len(self.subtasks)


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

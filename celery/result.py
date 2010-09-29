from __future__ import generators

import time

from copy import copy
from itertools import imap

from celery import states
from celery.backends import default_backend
from celery.datastructures import PositionQueue
from celery.exceptions import TimeoutError
from celery.messaging import with_connection
from celery.utils import any, all


class BaseAsyncResult(object):
    """Base class for pending result, supports custom task result backend.

    :param task_id: see :attr:`task_id`.
    :param backend: see :attr:`backend`.

    .. attribute:: task_id

        The unique identifier for this task.

    .. attribute:: backend

        The task result backend used.

    """

    TimeoutError = TimeoutError

    def __init__(self, task_id, backend):
        self.task_id = task_id
        self.backend = backend

    def forget(self):
        """Forget about (and possibly remove the result of) this task."""
        self.backend.forget(self.task_id)

    def revoke(self, connection=None, connect_timeout=None):
        """Send revoke signal to all workers.

        The workers will ignore the task if received.

        """
        from celery.task import control
        control.revoke(self.task_id, connection=connection,
                       connect_timeout=connect_timeout)

    def wait(self, timeout=None):
        """Wait for task, and return the result when it arrives.

        :keyword timeout: How long to wait, in seconds, before the
            operation times out.

        :raises celery.exceptions.TimeoutError: if ``timeout`` is not
            :const:`None` and the result does not arrive within ``timeout``
            seconds.

        If the remote call raised an exception then that
        exception will be re-raised.

        """
        return self.backend.wait_for(self.task_id, timeout=timeout)

    def get(self, timeout=None):
        """Alias to :meth:`wait`."""
        return self.wait(timeout=timeout)

    def ready(self):
        """Returns :const:`True` if the task executed successfully, or raised
        an exception.

        If the task is still running, pending, or is waiting
        for retry then :const:`False` is returned.

        """
        return self.status not in self.backend.UNREADY_STATES

    def successful(self):
        """Returns :const:`True` if the task executed successfully."""
        return self.status == states.SUCCESS

    def failed(self):
        """Returns :const:`True` if the task failed by exception."""
        return self.status == states.FAILURE

    def __str__(self):
        """``str(self) -> self.task_id``"""
        return self.task_id

    def __hash__(self):
        """``hash(self) -> hash(self.task_id)``"""
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

        If the task raised an exception, this will be the exception instance.

        """
        return self.backend.get_result(self.task_id)

    @property
    def traceback(self):
        """Get the traceback of a failed task."""
        return self.backend.get_traceback(self.task_id)

    @property
    def status(self):
        """The current status of the task.

        Can be one of the following:

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

    :param task_id: see :attr:`task_id`.


    .. attribute:: task_id

        The unique identifier for this task.

    .. attribute:: backend

        Instance of :class:`celery.backends.DefaultBackend`.

    """

    def __init__(self, task_id, backend=None):
        super(AsyncResult, self).__init__(task_id, backend or default_backend)


class TaskSetResult(object):
    """Working with :class:`~celery.task.TaskSet` results.

    An instance of this class is returned by
    ``TaskSet``'s :meth:`~celery.task.TaskSet.apply_async()`. It enables
    inspection of the subtasks status and return values as a single entity.

    :option taskset_id: see :attr:`taskset_id`.
    :option subtasks: see :attr:`subtasks`.

    .. attribute:: taskset_id

        The UUID of the taskset itself.

    .. attribute:: subtasks

        A list of :class:`AsyncResult` instances for all of the subtasks.

    """

    def __init__(self, taskset_id, subtasks):
        self.taskset_id = taskset_id
        self.subtasks = subtasks

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

    @with_connection
    def revoke(self, connection=None, connect_timeout=None):
        for subtask in self.subtasks:
            subtask.revoke(connection=connection)

    def __iter__(self):
        """``iter(res)`` -> ``res.iterate()``."""
        return self.iterate()

    def __getitem__(self, index):
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

    def join(self, timeout=None):
        """Gather the results for all of the tasks in the taskset,
        and return a list with them ordered by the order of which they
        were called.

        :keyword timeout: The time in seconds, how long
            it will wait for results, before the operation times out.

        :raises celery.exceptions.TimeoutError: if ``timeout`` is not
            :const:`None` and the operation takes longer than ``timeout``
            seconds.

        If any of the tasks raises an exception, the exception
        will be reraised by :meth:`join`.

        :returns: list of return values for all tasks in the taskset.

        """

        time_start = time.time()

        def on_timeout():
            raise TimeoutError("The operation timed out.")

        results = PositionQueue(length=self.total)

        while True:
            for position, pending_result in enumerate(self.subtasks):
                if pending_result.status == states.SUCCESS:
                    results[position] = pending_result.result
                elif pending_result.status in states.PROPAGATE_STATES:
                    raise pending_result.result
            if results.full():
                # Make list copy, so the returned type is not a position
                # queue.
                return list(results)
            else:
                if timeout is not None and \
                        time.time() >= time_start + timeout:
                    on_timeout()

    def save(self, backend=default_backend):
        """Save taskset result for later retrieval using :meth:`restore`.

        Example:

            >>> result.save()
            >>> result = TaskSetResult.restore(task_id)

        """
        backend.save_taskset(self.taskset_id, self)

    @classmethod
    def restore(self, taskset_id, backend=default_backend):
        """Restore previously saved taskset result."""
        return backend.restore_taskset(taskset_id)

    @property
    def total(self):
        """The total number of tasks in the :class:`~celery.task.TaskSet`."""
        return len(self.subtasks)


class EagerResult(BaseAsyncResult):
    """Result that we know has already been executed.  """
    TimeoutError = TimeoutError

    def __init__(self, task_id, ret_value, status, traceback=None):
        self.task_id = task_id
        self._result = ret_value
        self._status = status
        self._traceback = traceback

    def successful(self):
        """Returns :const:`True` if the task executed without failure."""
        return self.status == states.SUCCESS

    def ready(self):
        """Returns :const:`True` if the task has been executed."""
        return True

    def wait(self, timeout=None):
        """Wait until the task has been executed and return its result."""
        if self.status == states.SUCCESS:
            return self.result
        elif self.status in states.PROPAGATE_STATES:
            raise self.result

    def revoke(self):
        self._status = states.REVOKED

    @property
    def result(self):
        """The tasks return value"""
        return self._result

    @property
    def status(self):
        """The tasks status"""
        return self._status

    @property
    def traceback(self):
        """The traceback if the task failed."""
        return self._traceback

    def __repr__(self):
        return "<EagerResult: %s>" % self.task_id

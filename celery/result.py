"""

Asynchronous result types.

"""
from celery.backends import default_backend
from celery.datastructures import PositionQueue
from celery.exceptions import TimeoutError
from itertools import imap
import time


class BaseAsyncResult(object):
    """Base class for pending result, supports custom
    task meta :attr:`backend`

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

    def is_done(self):
        """Returns ``True`` if the task executed successfully.

        :rtype: bool

        """
        return self.backend.is_done(self.task_id)

    def get(self):
        """Alias to :meth:`wait`."""
        return self.wait()

    def wait(self, timeout=None):
        """Wait for task, and return the result when it arrives.

        :keyword timeout: How long to wait in seconds, before the
            operation times out.

        :raises celery.exceptions.TimeoutError: if ``timeout`` is not ``None``
            and the result does not arrive within ``timeout`` seconds.

        If the remote call raised an exception then that
        exception will be re-raised.

        """
        return self.backend.wait_for(self.task_id, timeout=timeout)

    def ready(self):
        """Returns ``True`` if the task executed successfully, or raised
        an exception. If the task is still pending, or is waiting for retry
        then ``False`` is returned.

        :rtype: bool

        """
        status = self.backend.get_status(self.task_id)
        return status not in ["PENDING", "RETRY"]

    def successful(self):
        """Alias to :meth:`is_done`."""
        return self.is_done()

    def __str__(self):
        """``str(self)`` -> ``self.task_id``"""
        return self.task_id

    def __repr__(self):
        return "<AsyncResult: %s>" % self.task_id

    @property
    def result(self):
        """When the task has been executed, this contains the return value.

        If the task raised an exception, this will be the exception instance.

        """
        if self.status == "DONE" or self.status == "FAILURE":
            return self.backend.get_result(self.task_id)
        return None

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

            *RETRY*

                The task is to be retried, possibly because of failure.

            *FAILURE*

                The task raised an exception, or has been retried more times
                than its limit. The :attr:`result` attribute contains the
                exception raised.

            *DONE*

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

    def __init__(self, task_id):
        super(AsyncResult, self).__init__(task_id, backend=default_backend)


class TaskSetResult(object):
    """Working with :class:`celery.task.TaskSet` results.

    An instance of this class is returned by
    :meth:`celery.task.TaskSet.run()`. It lets you inspect the status and
    return values of the taskset as a single entity.

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

        :returns: ``True`` if all of the tasks in the taskset finished
            successfully (i.e. did not raise an exception).

        """
        return all((subtask.successful()
                        for subtask in self.itersubtasks()))

    def failed(self):
        """Did the taskset fail?

        :returns: ``True`` if any of the tasks in the taskset failed.
            (i.e., raised an exception)

        """
        return any((not subtask.successful()
                        for subtask in self.itersubtasks()))

    def waiting(self):
        """Is the taskset waiting?

        :returns: ``True`` if any of the tasks in the taskset is still
            waiting for execution.

        """
        return any((not subtask.ready()
                        for subtask in self.itersubtasks()))

    def ready(self):
        """Is the task ready?

        :returns: ``True`` if all of the tasks in the taskset has been
            executed.

        """
        return all((subtask.ready()
                        for subtask in self.itersubtasks()))

    def completed_count(self):
        """Task completion count.

        :returns: the number of tasks completed.

        """
        return sum(imap(int, (subtask.successful()
                                for subtask in self.itersubtasks())))

    def __iter__(self):
        """``iter(res)`` -> ``res.iterate()``."""
        return self.iterate()

    def iterate(self):
        """Iterate over the return values of the tasks as they finish
        one by one.

        :raises: The exception if any of the tasks raised an exception.

        """
        results = dict((subtask.task_id, AsyncResult(subtask.task_id))
                            for subtask in self.subtasks)
        while results:
            for task_id, pending_result in results.items():
                if pending_result.status == "DONE":
                    del(results[task_id])
                    yield pending_result.result
                elif pending_result.status == "FAILURE":
                    raise pending_result.result

    def join(self, timeout=None):
        """Gather the results for all of the tasks in the taskset,
        and return a list with them ordered by the order of which they
        were called.

        :keyword timeout: The time in seconds, how long
            it will wait for results, before the operation times out.

        :raises celery.exceptions.TimeoutError: if ``timeout`` is not ``None``
            and the operation takes longer than ``timeout`` seconds.

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
                if pending_result.status == "DONE":
                    results[position] = pending_result.result
                elif pending_result.status == "FAILURE":
                    raise pending_result.result
            if results.full():
                # Make list copy, so the returned type is not a position
                # queue.
                return list(results)
            else:
                if timeout is not None and \
                        time.time() >= time_start + timeout:
                    on_timeout()

    @property
    def total(self):
        """The total number of tasks in the :class:`celery.task.TaskSet`."""
        return len(self.subtasks)


class EagerResult(BaseAsyncResult):
    """Result that we know has already been executed.  """
    TimeoutError = TimeoutError

    def __init__(self, task_id, ret_value, status, traceback=None):
        self.task_id = task_id
        self._result = ret_value
        self._status = status
        self._traceback = traceback

    def is_done(self):
        """Returns ``True`` if the task executed without failure."""
        return self.status == "DONE"

    def is_ready(self):
        """Returns ``True`` if the task has been executed."""
        return True

    def wait(self, timeout=None):
        """Wait until the task has been executed and return its result."""
        if self.status == "DONE":
            return self.result
        elif self.status == "FAILURE":
            raise self.result.exception

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

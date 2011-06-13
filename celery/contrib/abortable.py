"""
=========================
Abortable tasks overview
=========================

For long-running :class:`Task`'s, it can be desirable to support
aborting during execution. Of course, these tasks should be built to
support abortion specifically.

The :class:`AbortableTask` serves as a base class for all :class:`Task`
objects that should support abortion by producers.

* Producers may invoke the :meth:`abort` method on
  :class:`AbortableAsyncResult` instances, to request abortion.

* Consumers (workers) should periodically check (and honor!) the
  :meth:`is_aborted` method at controlled points in their task's
  :meth:`run` method. The more often, the better.

The necessary intermediate communication is dealt with by the
:class:`AbortableTask` implementation.

Usage example
-------------

In the consumer:

.. code-block:: python

   from celery.contrib.abortable import AbortableTask

   class MyLongRunningTask(AbortableTask):

       def run(self, **kwargs):
           logger = self.get_logger(**kwargs)
           results = []
           for x in xrange(100):
               # Check after every 5 loops..
               if x % 5 == 0:  # alternatively, check when some timer is due
                   if self.is_aborted(**kwargs):
                       # Respect the aborted status and terminate
                       # gracefully
                       logger.warning("Task aborted.")
                       return None
               y = do_something_expensive(x)
               results.append(y)
           logger.info("Task finished.")
           return results


In the producer:

.. code-block:: python

   from myproject.tasks import MyLongRunningTask

   def myview(request):

       async_result = MyLongRunningTask.delay()
       # async_result is of type AbortableAsyncResult

       # After 10 seconds, abort the task
       time.sleep(10)
       async_result.abort()

       ...

After the `async_result.abort()` call, the task execution is not
aborted immediately. In fact, it is not guaranteed to abort at all. Keep
checking the `async_result` status, or call `async_result.wait()` to
have it block until the task is finished.

.. note::

   In order to abort tasks, there needs to be communication between the
   producer and the consumer.  This is currently implemented through the
   database backend.  Therefore, this class will only work with the
   database backends.

"""
from celery.task.base import Task
from celery.result import AsyncResult


"""
Task States
-----------

.. state:: ABORTED

ABORTED
~~~~~~~

Task is aborted (typically by the producer) and should be
aborted as soon as possible.

"""
ABORTED = "ABORTED"


class AbortableAsyncResult(AsyncResult):
    """Represents a abortable result.

    Specifically, this gives the `AsyncResult` a :meth:`abort()` method,
    which sets the state of the underlying Task to `"ABORTED"`.

    """

    def is_aborted(self):
        """Returns :const:`True` if the task is (being) aborted."""
        return self.backend.get_status(self.task_id) == ABORTED

    def abort(self):
        """Set the state of the task to :const:`ABORTED`.

        Abortable tasks monitor their state at regular intervals and
        terminate execution if so.

        Be aware that invoking this method does not guarantee when the
        task will be aborted (or even if the task will be aborted at
        all).

        """
        # TODO: store_result requires all four arguments to be set,
        # but only status should be updated here
        return self.backend.store_result(self.task_id, result=None,
                                         status=ABORTED, traceback=None)


class AbortableTask(Task):
    """A celery task that serves as a base class for all :class:`Task`'s
    that support aborting during execution.

    All subclasses of :class:`AbortableTask` must call the
    :meth:`is_aborted` method periodically and act accordingly when
    the call evaluates to :const:`True`.

    """

    @classmethod
    def AsyncResult(cls, task_id):
        """Returns the accompanying AbortableAsyncResult instance."""
        return AbortableAsyncResult(task_id, backend=cls.backend)

    def is_aborted(self, **kwargs):
        """Checks against the backend whether this
        :class:`AbortableAsyncResult` is :const:`ABORTED`.

        Always returns :const:`False` in case the `task_id` parameter
        refers to a regular (non-abortable) :class:`Task`.

        Be aware that invoking this method will cause a hit in the
        backend (for example a database query), so find a good balance
        between calling it regularly (for responsiveness), but not too
        often (for performance).

        """
        result = self.AsyncResult(kwargs["task_id"])
        if not isinstance(result, AbortableAsyncResult):
            return False
        return result.is_aborted()

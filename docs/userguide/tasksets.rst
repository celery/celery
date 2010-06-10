=======================================
 Sets of tasks, Subtasks and Callbacks
=======================================

.. contents::
    :local:

Subtasks
========

The :class:`~celery.task.sets.subtask` class is used to wrap the arguments and
execution options for a single task invocation::

    subtask(task_name_or_cls, args, kwargs, options)

For convenience every task also has a shortcut to create subtask instances::

    task.subtask(args, kwargs, options)

:class:`~celery.task.sets.subtask` is actually a subclass of :class:`dict`,
which means it can be serialized with JSON or other encodings that doesn't
support complex Python objects.

Also it can be regarded as a type, as the following usage works::

    >>> s = subtask("tasks.add", args=(2, 2), kwargs={})

    >>> subtask(dict(s))  # coerce dict into subtask

This makes it excellent as a means to pass callbacks around to tasks.

Let's improve our ``add`` task so it can accept a callback that
takes the result as an argument::

    from celery.decorators import task
    from celery.task.sets import subtask

    @task
    def add(x, y, callback=None):
        result = x + y
        if callback is not None:
            subtask(callback).apply_async(result)
        return result

See? :class:`~celery.task.sets.subtask` also knows how it should be applied,
asynchronously by :meth:`~celery.task.sets.subtask.apply_async`, and
eagerly by :meth:`~celery.task.sets.subtask.apply`.

The best thing is that any arguments you add to ``subtask.apply_async``,
will be prepended to the arguments specified by the subtask itself!

:note: additional keyword arguments will be added to the
  execution options, not the task keyword arguments.

So if you have the subtask::

    >>> add.subtask(args=(10, ), options={"ignore_result": True})

``subtask.apply_async(result)`` becomes::

    >>> add.apply_async(args=(result, 10), ignore_result=True)

and ``subtask.apply_async(result, ignore_result=False)`` becomes::

    >>> add.apply_async(args=(result, 10), ignore_result=False)


Now let's execute our new ``add`` task with a callback::

    >>> add.delay(2, 2, callback=add.subtask((8, )))

As expected this will first launch one task calculating ``2 + 2``, then 
another task calculating ``4 + 8``.

TaskSets
=========

The :class:`~celery.task.sets.TaskSet` enables easy invocation of several
tasks at once, and is then able to join the results in the same order as the
tasks were invoked.

The task set works on a list of :class:`~celery.task.sets.subtask`'s::

    >>> from celery.task.sets import TaskSet
    >>> from tasks import add

    >>> job = TaskSet(tasks=[
    ...             add.subtask((4, 4)),
    ...             add.subtask((8, 8)),
    ...             add.subtask((16, 16)),
    ...             add.subtask((32, 32)),
    ... ])

    >>> result = job.apply_async()

    >>> result.ready()  # has all subtasks completed?
    True
    >>> result.successful() # was all subtasks successful?

    >>> result.join()
    [4, 8, 16, 32, 64]


TaskSet Results
===============

When a  :class:`~celery.task.sets.TaskSet` is applied it returns a
:class:`~celery.result.TaskSetResult` object.

:class:`~celery.result.TaskSetResult` takes a list of
:class:`~celery.result.AsyncResult` instances and operates on them as if it was a
single task.

It supports the following operations:

* :meth:`~celery.result.TaskSetResult.successful`

    Returns :const:`True` if all of the subtasks finished
    successfully (e.g. did not raise an exception).

* :meth:`~celery.result.TaskSetResult.failed`

    Returns :const:`True` if any of the subtasks failed.

* :meth:`~celery.result.TaskSetResult.waiting`

    Returns :const:`True` if any of the subtasks
    is not ready.

* :meth:`~celery.result.TaskSetResult.ready`

    Return :const:`True` if all of the subtasks
    are ready.

* :meth:`~celery.result.TaskSetResult.completed_count`

    Returns the number of completed subtasks.

* :meth:`~celery.result.TaskSetResult.revoke`

    Revoke all of the subtasks.

* :meth:`~celery.result.TaskSetResult.iterate`

    Iterate over the return values of the subtasks
    as they finish, one by one.

* :meth:`~celery.result.TaskSetResult.join`

    Gather the results for all of the subtasks,
    and return a list with them ordered by the order of which they
    were called.

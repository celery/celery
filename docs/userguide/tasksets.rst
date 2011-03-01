.. _guide-sets:

=======================================
 Sets of tasks, Subtasks and Callbacks
=======================================

.. contents::
    :local:

.. _sets-subtasks:

Subtasks
========

.. versionadded:: 2.0

The :class:`~celery.task.sets.subtask` type is used to wrap the arguments and
execution options for a single task invocation::

    subtask(task_name_or_cls, args, kwargs, options)

For convenience every task also has a shortcut to create subtasks::

    task.subtask(args, kwargs, options)

:class:`~celery.task.sets.subtask` is actually a :class:`dict` subclass,
which means it can be serialized with JSON or other encodings that doesn't
support complex Python objects.

Also it can be regarded as a type, as the following usage works::

    >>> s = subtask("tasks.add", args=(2, 2), kwargs={})

    >>> subtask(dict(s))  # coerce dict into subtask

This makes it excellent as a means to pass callbacks around to tasks.

.. _sets-callbacks:

Callbacks
---------

Let's improve our `add` task so it can accept a callback that
takes the result as an argument::

    from celery.task import task
    from celery.task.sets import subtask

    @task
    def add(x, y, callback=None):
        result = x + y
        if callback is not None:
            subtask(callback).delay(result)
        return result

:class:`~celery.task.sets.subtask` also knows how it should be applied,
asynchronously by :meth:`~celery.task.sets.subtask.delay`, and
eagerly by :meth:`~celery.task.sets.subtask.apply`.

The best thing is that any arguments you add to `subtask.delay`,
will be prepended to the arguments specified by the subtask itself!

If you have the subtask::

    >>> add.subtask(args=(10, ))

`subtask.delay(result)` becomes::

    >>> add.apply_async(args=(result, 10))

...

Now let's execute our new `add` task with a callback::

    >>> add.delay(2, 2, callback=add.subtask((8, )))

As expected this will first launch one task calculating :math:`2 + 2`, then
another task calculating :math:`4 + 8`.

.. _sets-taskset:

Task Sets
=========

The :class:`~celery.task.sets.TaskSet` enables easy invocation of several
tasks at once, and is then able to join the results in the same order as the
tasks were invoked.

A task set takes a list of :class:`~celery.task.sets.subtask`'s::

    >>> from celery.task.sets import TaskSet
    >>> from tasks import add

    >>> job = TaskSet(tasks=[
    ...             add.subtask((4, 4)),
    ...             add.subtask((8, 8)),
    ...             add.subtask((16, 16)),
    ...             add.subtask((32, 32)),
    ... ])

    >>> result = job.apply_async()

    >>> result.ready()  # have all subtasks completed?
    True
    >>> result.successful() # were all subtasks successful?
    True
    >>> result.join()
    [4, 8, 16, 32, 64]

.. _sets-results:

Results
-------

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
    is not ready yet.

* :meth:`~celery.result.TaskSetResult.ready`

    Return :const:`True` if all of the subtasks
    are ready.

* :meth:`~celery.result.TaskSetResult.completed_count`

    Returns the number of completed subtasks.

* :meth:`~celery.result.TaskSetResult.revoke`

    Revokes all of the subtasks.

* :meth:`~celery.result.TaskSetResult.iterate`

    Iterates over the return values of the subtasks
    as they finish, one by one.

* :meth:`~celery.result.TaskSetResult.join`

    Gather the results for all of the subtasks
    and return a list with them ordered by the order of which they
    were called.


Task set callbacks
------------------


Simple, but may take a long time before your callback is called:


.. code-block:: python

    from celery import current_app
    from celery.task import subtask

    def join_taskset(setid, subtasks, callback, interval=15, max_retries=None):
        result = TaskSetResult(setid, subtasks)
        if result.ready():
            return subtask(callback).delay(result.join())
        join_taskset.retry(countdown=interval, max_retries=max_retries)



Using Redis and atomic counters:


.. code-block:: python

    from celery import current_app
    from celery.task import Task, TaskSet
    from celery.result import TaskSetResult
    from celery.utils import gen_unique_id, cached_property
    from redis import Redis
    from time import sleep

    class supports_taskset_callback(Task):
        abstract = True
        accept_magic_kwargs = False

        def after_return(self, \*args, \*\*kwargs):
            if self.request.taskset:
                callback = self.request.kwargs.get("callback")
                if callback:
                    setid = self.request.taskset
                    # task set must be saved in advance, so the task doesn't
                    # try to restore it before that happens.  This is why we
                    # use the `apply_presaved_taskset` below.
                    result = TaskSetResult.restore(setid)
                    current = self.redis.incr("taskset-" + setid)
                    if current >= result.total:
                        r = subtask(callback).delay(result.join())

        @cached_property
        def redis(self):
            return Redis(host="localhost", port=6379)

    @task(base=supports_taskset_callback)
    def add(x, y, \*\*kwargs):
        return x + y

    @task
    def sum_of(numbers):
        print("TASKSET READY: %r" % (sum(numbers), ))

    def apply_presaved_taskset(tasks):
        r = []
        setid = gen_unique_id()
        for task in tasks:
            uuid = gen_unique_id()
            task.options["task_id"] = uuid
            r.append((task, current_app.AsyncResult(uuid)))
        ts = current_app.TaskSetResult(setid, [task[1] for task in r])
        ts.save()
        return TaskSet(task[0] for task in r).apply_async(taskset_id=setid)


    # sum of 100 add tasks
    result = apply_presaved_taskset(
                add.subtask((i, i), {"callback": sum_of.subtask()})
                    for i in xrange(100))

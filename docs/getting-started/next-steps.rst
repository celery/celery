.. _next-steps:

============
 Next Steps
============

The :ref:`first-steps` guide is intentionally minimal.  In this guide
we will demonstrate what Celery offers in more detail, including
how to add Celery support for your application and library.

.. contents::
    :local:
    :depth: 1

Using Celery in your Application
================================

.. _project-layout:

Our Project
-----------

Project layout::

    proj/__init__.py
        /celery.py
        /tasks.py

:file:`proj/celery.py`
~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/next-steps/proj/celery.py
    :language: python

In this module we created our :class:`@Celery` instance (sometimes
referred to as the *app*).  To use Celery within your project
you simply import this instance.

- The ``broker`` argument specifies the URL of the broker to use.

    See :ref:`celerytut-broker` for more information.

- The ``backend`` argument specifies the result backend to use,

    It's used to keep track of task state and results.
    While results are disabled by default we use the amqp backend here
    to demonstrate how retrieving the results work, you may want to use
    a different backend for your application, as they all have different
    strenghts and weaknesses.  If you don't need results it's best
    to disable them.  Results can also be disabled for individual tasks
    by setting the ``@task(ignore_result=True)`` option.

    See :ref:`celerytut-keeping-results` for more information.

- The ``include`` argument is a list of modules to import when
  the worker starts.  We need to add our tasks module here so
  that the worker is able to find our tasks.

:file:`proj/tasks.py`
~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/next-steps/proj/tasks.py
    :language: python


Starting the worker
-------------------

The :program:`celery` program can be used to start the worker::

    $ celery worker --app=proj -l info

When the worker starts you should see a banner and some messages::

     -------------- celery@halcyon.local v3.0 (Chiastic Slide)
     ---- **** -----
     --- * ***  * -- [Configuration]
     -- * - **** --- . broker:      amqp://guest@localhost:5672//
     - ** ---------- . app:         __main__:0x1012d8590
     - ** ---------- . concurrency: 8 (processes)
     - ** ---------- . events:      OFF (enable -E to monitor this worker)
     - ** ----------
     - *** --- * --- [Queues]
     -- ******* ---- . celery:      exchange:celery(direct) binding:celery
     --- ***** -----

     [2012-06-08 16:23:51,078: WARNING/MainProcess] celery@halcyon.local has started.

-- The *broker* is the URL you specifed in the broker argument in our ``celery``
module, you can also specify a different broker on the command line by using
the :option:`-b` option.

-- *Concurrency* is the number of multiprocessing worker process used
to process your tasks concurrently, when all of these are busy doing work
new tasks will have to wait for one of the tasks to finish before
it can be processed.

The default concurrency number is the number of CPU's on that machine
(including cores), you can specify a custom number using :option:`-c` option.
There is no recommended value, as the optimal number depends on a number of
factors, but if your tasks are mostly I/O-bound then you can try to increase
it, experimentation has shown that adding more than twice the number
of CPU's is rarely effective, and likely to degrade performance
instead.

Including the default multiprocessing pool, Celery also supports using
Eventlet, Gevent, and threads (see :ref:`concurrency`).

-- *Events* is an option that when enabled causes Celery to send
monitoring messages (events) for actions occurring in the worker.
These can be used by monitor programs like ``celery events``,
celerymon and the Django-Celery admin monitor that you can read
about in the :ref:`Monitoring and Management guide <guide-monitoring>`.

-- *Queues* is the list of queues that the worker will consume
tasks from.  The worker can be told to consume from several queues
at once, and this is used to route messages to specific workers
as a means for Quality of Service, separation of concerns,
and emulating priorities, all described in the :ref:`Routing Guide
<guide-routing>`.

You can get a complete list of command line arguments
by passing in the `--help` flag::

    $ celery worker --help

These options are described in more detailed in the :ref:`Workers Guide <guide-workers>`.


.. _app-argument:

About the :option:`--app` argument
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :option:`--app` argument specifies the Celery app instance to use,
it must be in the form of ``module.path:celery``, where the part before the colon
is the name of the module, and the attribute name comes last.
If a package name is specified instead it will automatically
try to find a ``celery`` module in that package, and if the name
is a module it will try to find a ``celery`` attribute in that module.
This means that these are all equal:

    $ celery --app=proj
    $ celery --app=proj.celery:
    $ celery --app=proj.celery:celery


.. _calling-tasks:

Calling Tasks
=============

You can call a task using the :meth:`delay` method::

    >>> add.delay(2, 2)

This method is actually a star-argument shortcut to another method called
:meth:`apply_async`::

    >>> add.apply_async((2, 2))

The latter enables you to specify execution options like the time to run
(countdown), the queue it should be sent to and so on::

    >>> add.apply_async((2, 2), queue='lopri', countdown=10)

In the above example the task will be sent to a queue named ``lopri`` and the
task will execute, at the earliest, 10 seconds after the message was sent.

Applying the task directly will execute the task in the current process,
so that no message is sent::

    >>> add(2, 2)
    4

These three methods - :meth:`delay`, :meth:`apply_async`, and applying
(``__call__``), represents the Celery calling API, which are also used for
subtasks.

A more detailed overview of the Calling API can be found in the
:ref:`Calling User Guide <guide-calling>`.

Every task invocation will be given a unique identifier (an UUID), this
is the task id.

The ``delay`` and ``apply_async`` methods return an :class:`~@AsyncResult`
instance, which can be used to keep track of the tasks execution state.
But for this you need to enable a :ref:`result backend <task-result-backends>` so that
the state can be stored somewhere.

Results are disabled by default because of the fact that there is no result
backend that suits every application, so to choose one you need to consider
the drawbacks of each individual backend.  For many tasks
keeping the return value isn't even very useful, so it's a sensible default to
have.  Also note that result backends are not used for monitoring tasks and workers,
for that we use dedicated event messages (see :ref:`guide-monitoring`).

If you have a result backend configured we can retrieve the return
value of a task::

    >>> res = add.delay(2, 2)
    >>> res.get(timeout=1)
    4

You can find the task's id by looking at the :attr:`id` attribute::

    >>> res.id
    d6b3aea2-fb9b-4ebc-8da4-848818db9114

We can also inspect the exception and traceback if the task raised an
exception, in fact ``result.get()`` will propagate any errors by default::

    >>> res = add.delay(2)
    >>> res.get(timeout=1)
    Traceback (most recent call last):
    File "<stdin>", line 1, in <module>
    File "/opt/devel/celery/celery/result.py", line 113, in get
        interval=interval)
    File "/opt/devel/celery/celery/backends/amqp.py", line 138, in wait_for
        raise self.exception_to_python(meta['result'])
    TypeError: add() takes exactly 2 arguments (1 given)

If you don't wish for the errors to propagate then you can disable that
by passing the ``propagate`` argument::

    >>> res.get(propagate=False)
    TypeError('add() takes exactly 2 arguments (1 given)',)

In this case it will return the exception instance raised instead,
and so to check whether the task succeeded or failed you will have to
use the corresponding methods on the result instance::

    >>> res.failed()
    True

    >>> res.successful()
    False

So how does it know if the task has failed or not?  It can find out by looking
at the tasks *state*::

    >>> res.state
    'FAILURE'

A task can only be in a single state, but it can progress through several
states. The stages of a typical task can be::

    PENDING -> STARTED -> SUCCESS

The started state is a special state that is only recorded if the
:setting:`CELERY_TRACK_STARTED` setting is enabled, or if the
``@task(track_started=True)`` option is set for the task.

The pending state is actually not a recorded state, but rather
the default state for any task id that is unknown, which you can see
from this example::

    >>> from proj.celery import celery

    >>> res = celery.AsyncResult('this-id-does-not-exist')
    >>> res.state
    'PENDING'

If the task is retried the stages can become even more complex,
e.g, for a task that is retried two times the stages would be::

    PENDING -> STARTED -> RETRY -> STARTED -> RETRY -> STARTED -> SUCCESS

To read more about task states you should see the :ref:`task-states` section
in the tasks user guide.

.. _designing-workflows:

*Canvas*: Designing Workflows
=============================

We just learned how to call a task using the tasks ``delay`` method,
and this is often all you need, but sometimes you may want to pass the
signature of a task invocation to another process or as an argument to another
function, for this Celery uses something called *subtasks*.

A subtask wraps the arguments and execution options of a single task
invocation in a way such that it can be passed to functions or even serialized
and sent across the wire.

You can create a subtask for the ``add`` task using the arguments ``(2, 2)``,
and a countdown of 10 seconds like this::

    >>> add.subtask((2, 2), countdown=10)
    tasks.add(2, 2)

There is also a shortcut using star arguments::

    >>> add.s(2, 2)
    tasks.add(2, 2)

and it also supports keyword arguments::

    >>> add.s(2, 2, debug=True)
    tasks.add(2, 2, debug=True)

From any subtask instance we can inspect the different fields::

    >>> s = add.subtask((2, 2), {'debug': True}, countdown=10)
    >>> s.args
    (2, 2)
    >>> s.kwargs
    {'debug': True}
    >>> s.options
    {'countdown': 10}

And there's that calling API again...
-------------------------------------

Subtask instances also supports the calling API, which means you can use
``delay``, ``apply_async``, or *calling* it directly.

But there is a difference in that the subtask may already have
an argument signature specified.  The ``add`` task takes two arguments,
so a subtask specifying two arguments would make a complete signature::

    >>> s1 = add.s(2, 2)
    >>> res = s2.delay()
    >>> res.get()
    4

But, you can also make incomplete signatures to create what we call
*partials*::

    # incomplete partial:  add(?, 2)
    >>> s2 = add.s(2)

``s2`` is now a partial subtask that needs another argument to be complete,
and this can be resolved when calling the subtask::

    # resolves the partial: add(8, 2)
    >>> res = s2.delay(8)
    >>> res.get()
    10

Here we added the argument 8, which was prepended to the existing argument 2
forming a complete signature of ``add(8, 2)``.

Keyword arguments can also be added later, these are then merged with any
existing keyword arguments, but with new arguments taking precedence::

    >>> s3 = add.s(2, 2, debug=True)
    >>> s3.delay(debug=False)   # debug is now False.

As stated subtasks supports the calling API, which means that:

- ``subtask.apply_async(args=(), kwargs={}, **options)``

    Calls the subtask with optional partial arguments and partial
    keyword arguments.  Also supports partial execution options.

- ``subtask.delay(*args, **kwargs)``

  Star argument version of ``apply_async``.  Any arguments will be prepended
  to the arguments in the signature, and keyword arguments is merged with any
  existing keys.

So this all seems very useful, but what can we actually do with these?
To get to that we must introduce the canvas primitives...

The Primitives
--------------

- ``group``

    The group primitive is a subtask that takes a list of tasks that should
    be applied in parallel.

- ``chain``

    The chain primitive lets us link together subtasks so that one is called
    after the other, essentially forming a *chain* of callbacks.

- ``chord``

    A chord is just like a group but with a callback.  A group consists
    of a header group and a body,  where the body is a task that should execute
    after all of the tasks in the header is complete.

- ``map``

    The map primitive works like the built-in ``map`` function, but creates
    a temporary task where a list of arguments is applied to the task.
    E.g. ``task.map([1, 2])`` results in a single task
    being called, appyling the arguments in order to the task function so
    that the result is::

        res = [task(1), task(2)]

- ``starmap``

    Works exactly like map except the arguments are applied as ``*args``.
    For example ``add.starmap([(2, 2), (4, 4)])`` results in a single
    task calling::

        res = [add(2, 2), add(4, 4)]

- ``chunks``

    Chunking splits a long list of arguments into parts, e.g the operation::

        >>> add.chunks(zip(xrange(1000), xrange(1000), 10))

    will create 10 tasks that apply 100 items each.


The primitives are also subtasks themselves, so that they can be combined
in any number of ways to compose complex workflows.

Here's some examples::

- Simple chain

    Here's a simple chain, the first task executes passing its return value
    to the next task in the chain, and so on.

    .. code-block:: python

        # 2 + 2 + 4 + 8
        >>> res = chain(add.s(2, 2), add.s(4), add.s(8))()
        >>> res.get()
        16

    This can also be written using pipes::

        >>> (add.s(2, 2) | add.s(4) | add.s(8))().get()
        16

- Immutable subtasks

    As we have learned signatures can be partial, so that arguments can be
    added to the existing arguments, but you may not always want that,
    for example if you don't want the result of the previous task in a chain.

    In that case you can mark the subtask as immutable, so that the arguments
    cannot be changed::

        >>> add.subtask((2, 2), immutable=True)

    There's also an ``.si`` shortcut for this::

        >>> add.si(2, 2)

    Now we can create a chain of independent tasks instead::

        >>> res = (add.si(2, 2), add.si(4, 4), add.s(8, 8))()
        >>> res.get()
        16

        >>> res.parent.get()
        8

        >>> res.parent.parent.get()
        4

- Simple group

    We can easily create a group of tasks to execute in parallel::

        >>> res = group(add.s(i, i) for i in xrange(10))()
        >>> res.get(timeout=1)
        [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

    - For primitives `.apply_async` is special...

        as it will create a temporary task to apply the tasks in,
        for example by *applying the group*::

            >>> g = group(add.s(i, i) for i in xrange(10))
            >>> g()  # << applying

        the act of sending the messages for the tasks in the group
        will happen in the current process,
        but with ``.apply_async`` this happens in a temporary task
        instead::

            >>> g = group(add.s(i, i) for i in xrange(10))
            >>> g.apply_async()

        This is useful because we can e.g. specify a time for the
        messages in the group to be called::

            >>> g.apply_async(countdown=10)

- Simple chord

    The chord primitive enables us to add callback to be called when
    all of the tasks in a group has finished executing, which is often
    required for algorithms that aren't embarrassingly parallel::

        >>> res = chord((add.s(i, i) for i in xrange(10)), xsum.s())()
        >>> res.get()
        90

    The above example creates 10 task that all start in parallel,
    and when all of them is complete the return values is combined
    into a list and sent to the ``xsum`` task.

    The body of a chord can also be immutable, so that the return value
    of the group is not passed on to the callback::

        >>> chord((import_contact.s(c) for c in contacts),
        ...       notify_complete.si(import_id)).apply_async()

    Note the use of ``.si`` above which creates an immutable subtask.

- Blow your mind by combining

    Chains can be partial too::

        >>> c1 = (add.s(4) | mul.s(8))

        # (16 + 4) * 8
        >>> res = c1(16)
        >>> res.get()
        160

    Which means that you can combine chains::

        # ((4 + 16) * 2 + 4) * 8
        >>> c2 = (add.s(4, 16) | mul.s(2) | (add.s(4) | mul.s(8)))

        >>> res = c2()
        >>> res.get()
        352

    Chaining a group together with another task will automatically
    upgrade it to be a chord::

        >>> c3 = (group(add.s(i, i) for i in xrange(10) | xsum.s()))
        >>> res = c3()
        >>> res.get()
        90

    Groups and chords accepts partial arguments too, so in a chain
    the return value of the previous task is forwarded to all tasks in the group::


        >>> new_user_workflow = (create_user.s() | group(
        ...                      import_contacts.s(),
        ...                      send_welcome_email.s()))
        ... new_user_workflow.delay(username='artv',
        ...                         first='Art',
        ...                         last='Vandelay',
        ...                         email='art@vandelay.com')


Be sure to read more about workflows in the :ref:`Canvas <guide-canvas>` user
guide.



**This document is incomplete - and ends here :(**

.. _whatsnew-2.6:

==========================
 What's new in Celery 2.6
==========================

Celery aims to be a flexible and reliable, best-of-breed solution
to process vast amounts of messages in a distributed fashion, while
providing operations with the tools to maintain such a system.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should visit our `website`_.

While this version is backward compatible with previous versions
it is important that you read the following section.

If you use Celery in combination with Django you must also
read the `django-celery changelog`_ and upgrade to `django-celery 2.6`_.

This version is officially supported on CPython 2.5, 2.6, 2.7, 3.2 and 3.3,
as well as PyPy and Jython.


.. _`website`: http://celeryproject.org/
.. _`django-celery changelog`: http://bit.ly/djcelery-26-changelog
.. _`django-celery 2.6`: http://pypi.python.org/pypi/django-celery/

.. contents::
    :local:

.. _v260-important:

Important Notes
===============

Eventloop
---------

The worker is now running *without threads* when used with AMQP or Redis as a
broker, resulting in::

    - Much better performance overall.
    - Fixes several edge case race conditions.
    - Sub-millisecond timer precision.
    - Faster shutdown times.

The transports supported are:  ``amqplib``, ``librabbitmq``, and ``redis``
Hopefully this can be extended to include additional broker transports
in the future.

For increased reliability the :setting:`CELERY_FORCE_EXECV` setting is enabled
by default if the eventloop is not used.

Now depends on :mod:`billiard`.
-------------------------------

Billiard is a fork of the multiprocessing containing
the no-execv patch by sbt (http://bugs.python.org/issue8713),
and also contains the pool improvements previously located in Celery.

This fork was necessary as changes to the C extension code was required
for the no-execv patch to work.

- Issue #625
- Issue #627
- Issue #640
- `django-celery #122 <http://github.com/celery/django-celery/issues/122`
- `django-celery #124 <http://github.com/celery/django-celery/issues/122`

Last version to support Python 2.5
----------------------------------

The 2.6 series will be last series to support Python 2.5.

With several other distributions taking the step to discontinue
Python 2.5 support, we feel that it is time too.

Python 2.6 should be widely available at this point, and we urge
you to upgrade, but if that is not possible you still have the option
to continue using the Celery 2.6 series, and important bug fixes
introduced in Celery 2.7 will be back-ported to Celery 2.6 upon request.

.. _v260-news:

News
====

Chaining Tasks
--------------

Tasks can now have callbacks and errbacks, and dependencies are recorded

- The task message format have been updated with two new extension keys

    Both keys can be empty/undefined or a list of subtasks.

    - ``callbacks``

        Applied if the task exits successfully, with the result
        of the task as an argument.

    - ``errbacks``

        Applied if an error occurred while executing the task,
        with the uuid of the task as an argument.  Since it may not be possible
        to serialize the exception instance, it passes the uuid of the task
        instead.  The uuid can then be used to retrieve the exception and
        traceback of the task from the result backend.

    - ``link`` and ``link_error`` keyword arguments has been added
      to ``apply_async``.

        The value passed can be either a subtask or a list of
        subtasks:

        .. code-block:: python

            add.apply_async((2, 2), link=mul.subtask())
            add.apply_async((2, 2), link=[mul.subtask(), echo.subtask()])

        Example error callback:

        .. code-block:: python

            @task
            def error_handler(uuid):
                result = AsyncResult(uuid)
                exc = result.get(propagate=False)
                print("Task %r raised exception: %r\n%r" % (
                    exc, result.traceback))

            >>> add.apply_async((2, 2), link_error=error_handler)

    - We now track what subtasks a task sends, and some result backends
      supports retrieving this information.

        - task.request.children

            Contains the result instances of the subtasks
            the currently executing task has applied.

        - AsyncResult.children

            Returns the tasks dependencies, as a list of
            ``AsyncResult``/``ResultSet`` instances.

        - AsyncResult.iterdeps

            Recursively iterates over the tasks dependencies,
            yielding `(parent, node)` tuples.

            Raises IncompleteStream if any of the dependencies
            has not returned yet.

       - AsyncResult.graph

            A ``DependencyGraph`` of the tasks dependencies.
            This can also be used to convert to dot format:

            .. code-block:: python

                with open("graph.dot") as fh:
                    result.graph.to_dot(fh)

            which can than be used to produce an image::

                $ dot -Tpng graph.dot -o graph.png

- A new special subtask called ``chain`` is also included::

    .. code-block:: python

        >>> from celery import chain

        # (2 + 2) * 8 / 2
        >>> res = chain(add.subtask((2, 2)),
                        mul.subtask((8, )),
                        div.subtask((2,))).apply_async()
        >>> res.get() == 16

        >>> res.parent.get() == 32

        >>> res.parent.parent.get() == 4

- Adds :meth:`AsyncResult.get_leaf`

    Waits and returns the result of the leaf subtask.
    That is the last node found when traversing the graph,
    but this means that the graph can be 1-dimensional only (in effect
    a list).

- Adds ``subtask.link(subtask)`` + ``subtask.link_error(subtask)``

    Shortcut to ``s.options.setdefault("link", []).append(subtask)``

- Adds ``subtask.flatten_links()``

    Returns a flattened list of all dependencies (recursively)

`group`/`chord`/`chain` are now subtasks
----------------------------------------

- The source code for these, including subtask, has been moved
  to new module celery.canvas.

- group is no longer an alias to TaskSet, but new alltogether,
  since it was very difficult to migrate the TaskSet class to become
  a subtask.

- A new shortcut has been added to tasks::

        >>> task.s(arg1, arg2, kw=1)

    as a shortcut to::

        >>> task.subtask((arg1, arg2), {"kw": 1})

- Tasks can be chained by using the ``|`` operator::

        >>> (add.s(2, 2), pow.s(2)).apply_async()

- Subtasks can be "evaluated" using the ``~`` operator::

        >>> ~add.s(2, 2)
        4

        >>> ~(add.s(2, 2) | pow.s(2))

    is the same as::

        >>> chain(add.s(2, 2), pow.s(2)).apply_async().get()

- A new subtask_type key has been added to the subtask dicts

    This can be the string "chord", "group", "chain", "chunks",
    "xmap", or "xstarmap".

- maybe_subtask now uses subtask_type to reconstruct
  the object, to be used when using non-pickle serializers.

- The logic for these operations have been moved to dedicated
  tasks celery.chord, celery.chain and celery.group.

- subtask no longer inherits from AttributeDict.

    It's now a pure dict subclass with properties for attribute
    access to the relevant keys.

- The repr's now outputs how the sequence would like imperatively::

        >>> from celery import chord

        >>> (chord([add.s(i, i) for i in xrange(10)], xsum.s())
              | pow.s(2))
        tasks.xsum([tasks.add(0, 0),
                    tasks.add(1, 1),
                    tasks.add(2, 2),
                    tasks.add(3, 3),
                    tasks.add(4, 4),
                    tasks.add(5, 5),
                    tasks.add(6, 6),
                    tasks.add(7, 7),
                    tasks.add(8, 8),
                    tasks.add(9, 9)]) | tasks.pow(2)


Crontab now supports Day of Month, and Month of Year arguments
--------------------------------------------------------------

See the updated list of examples at :ref:`beat-crontab`.

Immutable subtasks
------------------

``subtask``'s can now be immutable, which means that the arguments
will not be modified when applying callbacks::

    >>> chain(add.s(2, 2), clear_static_electricity.si())

means it will not receive the argument of the parent task,
and ``.si()`` is a shortcut to::

    >>> clear_static_electricity.subtask(immutable=True)

Logging Improvements
--------------------

Logging support now conforms better with best practices.

- Classes used by the worker no longer uses app.get_default_logger, but uses
  `celery.utils.log.get_logger` which simply gets the logger not setting the
  level, and adds a NullHandler.

- Loggers are no longer passed around, instead every module using logging
  defines a module global logger that is used throughout.

- All loggers inherit from a common logger called "celery".

- Before task.get_logger would setup a new logger for every task,
  and even set the loglevel.  This is no longer the case.

    - Instead all task loggers now inherit from a common "celery.task" logger
      that is set up when programs call `setup_logging_subsystem`.

    - Instead of using LoggerAdapter to augment the formatter with
      the task_id and task_name field, the task base logger now use
      a special formatter adding these values at runtime from the
      currently executing task.

- In fact, ``task.get_logger`` is no longer recommended, it is better
  to add module-level logger to your tasks module.

    For example, like this:

    .. code-block:: python

        from celery.utils.log import get_task_logger

        logger = get_task_logger(__name__)

        @celery.task()
        def add(x, y):
            logger.debug("Adding %r + %r" % (x, y))
            return x + y

    The resulting logger will then inherit from the ``"celery.task"`` logger
    so that the current task name and id is included in logging output.

- Redirected output from stdout/stderr is now logged to a "celery.redirected"
  logger.

- In addition a few warnings.warn have been replaced with logger.warn.

- Now avoids the 'no handlers for logger multiprocessing' warning

Task registry no longer global
------------------------------

Every Celery instance now has its own task registry.

You can make apps share registries by specifying it::

    >>> app1 = Celery()
    >>> app2 = Celery(tasks=app1.tasks)

Note that tasks are shared between registries by default, so that
tasks will be added to every subsequently created task registry.
As an alternative tasks can be private to specific task registries
by setting the ``shared`` argument to the ``@task`` decorator::

    @celery.task(shared=False)
    def add(x, y):
        return x + y


Abstract tasks are now lazily bound.
------------------------------------

The :class:`~celery.task.Task` class is no longer bound to an app
by default, it will first be bound (and configured) when
a concrete subclass is created.

This means that you can safely import and make task base classes,
without also initializing the default app environment::

    from celery.task import Task

    class DebugTask(Task):
        abstract = True

        def __call__(self, *args, **kwargs):
            print("CALLING %r" % (self, ))
            return self.run(*args, **kwargs)

    >>> DebugTask
    <unbound DebugTask>

    >>> @celery1.task(base=DebugTask)
    ... def add(x, y):
    ...     return x + y
    >>> add.__class__
    <class add of <Celery default:0x101510d10>>


Lazy task decorators
--------------------

The ``@task`` decorator is now lazy when used with custom apps.

That is, if ``accept_magic_kwargs`` is enabled (herby called "compat mode"), the task
decorator executes inline like before, however for custom apps the @task
decorator now returns a special PromiseProxy object that is only evaluated
on access.

All promises will be evaluated when `app.finalize` is called, or implicitly
when the task registry is first used.


Smart `--app` option
--------------------

The :option:`--app` option now 'auto-detects'

    - If the provided path is a module it tries to get an
      attribute named 'celery'.

    - If the provided path is a package it tries
      to import a submodule named 'celery',
      and get the celery attribute from that module.

E.g. if you have a project named 'proj' where the
celery app is located in 'from proj.celery import celery',
then the following will be equivalent::

        $ celeryd --app=proj
        $ celeryd --app=proj.celery:
        $ celeryd --app=proj.celery:celery

In Other News
-------------

- New :setting:`CELERYD_WORKER_LOST_WAIT` to control the timeout in
  seconds before :exc:`billiard.WorkerLostError` is raised
  when a worker can not be signalled (Issue #595).

    Contributed by Brendon Crawford.

- Redis event monitor queues are now automatically deleted (Issue #436).

- App instance factory methods have been converted to be cached
  descriptors that creates a new subclass on access.

    This means that e.g. ``celery.Worker`` is an actual class
    and will work as expected when::

        class Worker(celery.Worker):
            ...

- New signal: :signal:`task-success`.

- Multiprocessing logs are now only emitted if the :envvar:`MP_LOG`
  environment variable is set.

- The Celery instance can now be created with a broker URL

    .. code-block:: python

        celery = Celery(broker="redis://")

- Result backends can now be set using an URL

    Currently only supported by redis.  Example use::

        CELERY_RESULT_BACKEND = "redis://localhost/1"

- Heartbeat frequency now every 5s, and frequency sent with event

    The heartbeat frequency is now available in the worker event messages,
    so that clients can decide when to consider workers offline based on
    this value.

- Module celery.actors has been removed, and will be part of cl instead.

- Introduces new ``celery`` command, which is an entrypoint for all other
  commands.

    The main for this command can be run by calling ``celery.start()``.

- Annotations now supports decorators if the key startswith '@'.

    E.g.:

    .. code-block:: python

        def debug_args(fun):

            @wraps(fun)
            def _inner(*args, **kwargs):
                print("ARGS: %r" % (args, ))
            return _inner

        CELERY_ANNOTATIONS = {
            "tasks.add": {"@__call__": debug_args},
        }

    Also tasks are now always bound by class so that
    annotated methods end up being bound.

- Bugreport now available as a command and broadcast command

    - Get it from a Python repl::

        >>> import celery
        >>> print(celery.bugreport())

    - Use celeryctl::

        $ celeryctl report

    - Get it from remote workers::

        $ celeryctl inspect report

- Module ``celery.log`` moved to :mod:`celery.app.log`.
- Module ``celery.task.control`` moved to :mod:`celery.app.control`.

- ``AsyncResult.task_id`` renamed to ``AsyncResult.id``

- ``TasksetResult.taskset_id`` renamed to ``.id``

- ``xmap(task, sequence)`` and ``xstarmap(task, sequence)``

    Returns a list of the results applying the task to every item
    in the sequence.

    Example::

        >>> from celery import xstarmap

        >>> xstarmap(add, zip(range(10), range(10)).apply_async()
        [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]

- ``chunks(task, sequence, chunksize)``

- ``group.skew(start=, stop=, step=)``

  Skew will skew the countdown for the individual tasks in a group,
  e.g. with a group::

        >>> g = group(add.s(i, i) for i in xrange(10))

  Skewing the tasks from 0 seconds to 10 seconds::

        >>> g.skew(stop=10)

  Will have the first task execute in 0 seconds, the second in 1 second,
  the third in 2 seconds and so on.

- 99% test Coverage

- :setting:`CELERY_QUEUES` can now be a list/tuple of :class:`~kombu.Queue`
  instances.

    Internally :attr:`@amqp.queues` is now a mapping of name/Queue instances,
    instead of converting on the fly.

* Can now specify connection for :class:`@control.inspect`.

    .. code-block:: python

        i = celery.control.inspect(connection=BrokerConnection("redis://"))
        i.active_queues()

* Module :mod:`celery.app.task` is now a module instead of a package.

    The setup.py install script will try to remove the old package,
    if that doesn't work for some reason you have to remove
    it manually, you can do so by executing the command::

        $ rm -r $(dirname $(python -c '
            import celery;print(celery.__file__)'))/app/task/

* :setting:`CELERY_FORCE_EXECV` is now enabled by default.

    If the old behavior is wanted the setting can be set to False,
    or the new :option:`--no-execv` to :program:`celeryd`.

* Deprecated module ``celery.conf`` has been removed.

* The :setting:`CELERY_TIMEZONE` now always require the :mod:`pytz`
  library to be installed (exept if the timezone is set to `UTC`).

* The Tokyo Tyrant backend has been removed and is no longer supported.

* Now uses :func:`~kombu.common.maybe_declare` to cache queue declarations.

* There is no longer a global default for the
  :setting:`CELERYBEAT_MAX_LOOP_INTERVAL` setting, it is instead
  set by individual schedulers.

* celeryd now truncates very long message bodies in error reports.

* :envvar:`CELERY_BENCH` environment variable, will now also list
  memory usage statistics at celeryd shutdown.

* celeryd now only ever use a single timer for all timing needs,
  and instead set different priorities.

Internals
---------

* Compat modules are now generated dynamically upon use.

    These modules are ``celery.messaging``, ``celery.log``,
    ``celery.decorators`` and ``celery.registry``.

* :mod:`celery.utils` refactored into multiple modules:

    :mod:`celery.utils.text`
    :mod:`celery.utils.imports`
    :mod:`celery.utils.functional`

* Now using :mod:`kombu.utils.encoding` instead of
  `:mod:`celery.utils.encoding`.

* Renamed module ``celery.routes`` -> :mod:`celery.app.routes`.

* Renamed package ``celery.db`` -> :mod:`celery.backends.database`.

* Renamed module ``celery.abstract`` -> :mod:`celery.worker.abstract`.

* Command-line docs are now parsed from the module docstrings.

* Test suite directory has been reorganized.

* :program:`setup.py` now reads docs from the :file:`requirements/` directory.

.. _v260-experimental:

Experimental
============

:mod:`celery.contrib.methods`:  Task decorator for methods
----------------------------------------------------------

This is an experimental module containing a task
decorator, and a task decorator filter, that can be used
to create tasks out of methods::

    from celery.contrib.methods import task_method

    class Counter(object):

        def __init__(self):
            self.value = 1

        @celery.task(name="Counter.increment", filter=task_method)
        def increment(self, n=1):
            self.value += 1
            return self.value


See :mod:`celery.contrib.methods` for more information.

.. _v260-unscheduled-removals:

Unscheduled Removals
====================

Usually we don't make backward incompatible removals,
but these removals should have no major effect.

- The following settings have been renamed:

    - ``CELERYD_ETA_SCHEDULER`` -> ``CELERYD_TIMER``
    - ``CELERYD_ETA_SCHEDULER_PRECISION`` -> ``CELERYD_TIMER_PRECISION``

.. _v260-deprecations:

Deprecations
============

See the :ref:`celery-deprecation-timeline`.

Fixes
=====

- Retry sqlalchemy backend operations on DatabaseError/OperationalError
  (Issue #634)

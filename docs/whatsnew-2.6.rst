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
.. _`django-celery 2.5`: http://pypi.python.org/pypi/django-celery/

.. contents::
    :local:

.. _v260-important:

Important Notes
===============

* Task registry is no longer a global.

* celery.task.Task is no longer bound to an app by default,
  so configuration of the task is lazy.

* The @task decorator is now lazy when used with custom apps

    If ``accept_magic_kwargs`` is enabled (herby called "compat mode"), the task
    decorator executes inline like before, however for custom apps the @task
    decorator now returns a special PromiseProxy object that is only evaluated
    on access.

    All promises will be evaluated when `app.finalize` is called, or implicitly
    when the task registry is first used.

* chain: Chain tasks together using callbacks under the hood.

    .. code-block:: python

        from celery import chain

        # (2 + 2) * 8 / 2
        res = chain(add.subtask((4, 4)),
                    mul.subtask((8, )),
                    div.subtask((2,))).apply_async()
        res.get() == 16

        res.parent.get() == 32

        res.parent.parent.get() == 4


* The Celery instance can now be created with a broker URL

    .. code-block:: python

        celery = Celery(broker="redis://")

* Result backends can now be set using an URL

    Currently only supported by redis.  Example use::

        CELERY_RESULT_BACKEND = "redis://localhost/1"

* Heartbeat frequency now every 5s, and frequency sent with event

    The heartbeat frequency is now available in the worker event messages,
    so that clients can decide when to consider workers offline based on
    this value.

* Module celery.actors has been removed, and will be part of cl instead.

* Introduces new ``celery`` command, which is an entrypoint for all other
  commands.

    The main for this command can be run by calling ``celery.start()``.

* Tasks can now have callbacks and errbacks, and dependencies are recorded

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

* Bugreport now available as a command and broadcast command

    - Get it from a Python repl::

        >>> import celery
        >>> print(celery.bugreport())

    - Use celeryctl::

        $ celeryctl report

    - Get it from remote workers::

        $ celeryctl inspect report

* Module ``celery.log`` moved to :mod:`celery.app.log`.
* Module ``celery.task.control`` moved to :mod:`celery.app.control`.

* Adds :meth:`AsyncResult.get_leaf`

    Waits and returns the result of the leaf subtask.
    That is the last node found when traversing the graph,
    but this means that the graph can be 1-dimensional only (in effect
    a list).

* Adds ``subtask.link(subtask)`` + ``subtask.link_error(subtask)``

    Shortcut to ``s.options.setdefault("link", []).append(subtask)``

* Adds ``subtask.flatten_links()``

    Returns a flattened list of all dependencies (recursively)

* ``AsyncResult.task_id`` renamed to ``AsyncResult.id``

* ``TasksetResult.taskset_id`` renamed to ``.id``




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

.. _v260-deprecations:

Deprecations
============

.. _v260-news:

News
====

In Other News
-------------

- Now depends on Kombu 2.1.4

Fixes
=====


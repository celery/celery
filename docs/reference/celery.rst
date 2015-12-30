===========================================
 :mod:`celery` --- Distributed processing
===========================================

.. currentmodule:: celery
.. module:: celery
    :synopsis: Distributed processing
.. moduleauthor:: Ask Solem <ask@celeryproject.org>
.. sectionauthor:: Ask Solem <ask@celeryproject.org>

--------------

This module is the main entry-point for the Celery API.
It includes commonly needed things for calling tasks,
and creating Celery applications.

===================== ===================================================
:class:`Celery`       celery application instance
:class:`group`        group tasks together
:class:`chain`        chain tasks together
:class:`chord`        chords enable callbacks for groups
:class:`signature`    object describing a task invocation
:data:`current_app`   proxy to the current application instance
:data:`current_task`  proxy to the currently executing task
===================== ===================================================

:class:`Celery` application objects
-----------------------------------

.. versionadded:: 2.5

.. autoclass:: Celery


    .. autoattribute:: user_options

    .. autoattribute:: steps

    .. autoattribute:: current_task

    .. autoattribute:: amqp

    .. autoattribute:: backend

    .. autoattribute:: loader

    .. autoattribute:: control
    .. autoattribute:: events
    .. autoattribute:: log
    .. autoattribute:: tasks
    .. autoattribute:: pool
    .. autoattribute:: Task
    .. autoattribute:: timezone

    .. automethod:: close

    .. automethod:: signature

    .. automethod:: bugreport

    .. automethod:: config_from_object

    .. automethod:: config_from_envvar

    .. automethod:: autodiscover_tasks

    .. automethod:: add_defaults

    .. automethod:: setup_security

    .. automethod:: start

    .. automethod:: task

    .. automethod:: send_task

    .. autoattribute:: AsyncResult

    .. autoattribute:: GroupResult

    .. automethod:: worker_main

    .. autoattribute:: Worker

    .. autoattribute:: WorkController

    .. autoattribute:: Beat

    .. automethod:: connection

    .. automethod:: connection_or_acquire

    .. automethod:: producer_or_acquire

    .. automethod:: mail_admins

    .. automethod:: select_queues

    .. automethod:: now

    .. automethod:: set_current

    .. automethod:: finalize

    .. data:: on_configure

        Signal sent when app is loading configuration.

    .. data:: on_after_configure

        Signal sent after app has prepared the configuration.

    .. data:: on_after_finalize

        Signal sent after app has been finalized.

Canvas primitives
-----------------

See :ref:`guide-canvas` for more about creating task workflows.

.. class:: group(task1[, task2[, task3[,… taskN]]])

    Creates a group of tasks to be executed in parallel.

    Example::

        >>> res = group([add.s(2, 2), add.s(4, 4)])()
        >>> res.get()
        [4, 8]

    A group is lazy so you must call it to take action and evaluate
    the group.

    Will return a `group` task that when called will then call all of the
    tasks in the group (and return a :class:`GroupResult` instance
    that can be used to inspect the state of the group).

.. class:: chain(task1[, task2[, task3[,… taskN]]])

    Chains tasks together, so that each tasks follows each other
    by being applied as a callback of the previous task.

    If called with only one argument, then that argument must
    be an iterable of tasks to chain.

    Example::

        >>> res = chain(add.s(2, 2), add.s(4))()

    is effectively :math:`(2 + 2) + 4)`::

        >>> res.get()
        8

    Calling a chain will return the result of the last task in the chain.
    You can get to the other tasks by following the ``result.parent``'s::

        >>> res.parent.get()
        4

.. class:: chord(header[, body])

    A chord consists of a header and a body.
    The header is a group of tasks that must complete before the callback is
    called.  A chord is essentially a callback for a group of tasks.

    Example::

        >>> res = chord([add.s(2, 2), add.s(4, 4)])(sum_task.s())

    is effectively :math:`\Sigma ((2 + 2) + (4 + 4))`::

        >>> res.get()
        12

    The body is applied with the return values of all the header
    tasks as a list.

.. class:: signature(task=None, args=(), kwargs={}, options={})

    Describes the arguments and execution options for a single task invocation.

    Used as the parts in a :class:`group` or to safely pass
    tasks around as callbacks.

    Signatures can also be created from tasks::

        >>> add.signature(args=(), kwargs={}, options={})

    or the ``.s()`` shortcut::

        >>> add.s(*args, **kwargs)

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to :meth:`Task.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = signature("tasks.add", args=(2, 2))
        >>> signature(s)
        {"task": "tasks.add", args=(2, 2), kwargs={}, options={}}

    .. method:: signature.__call__(*args \*\*kwargs)

        Call the task directly (in the current process).

    .. method:: signature.delay(*args, \*\*kwargs)

        Shortcut to :meth:`apply_async`.

    .. method:: signature.apply_async(args=(), kwargs={}, …)

        Apply this task asynchronously.

        :keyword args: Partial args to be prepended to the existing args.
        :keyword kwargs: Partial kwargs to be merged with the existing kwargs.
        :keyword options: Partial options to be merged with the existing
                          options.

        See :meth:`~@Task.apply_async`.

    .. method:: signature.apply(args=(), kwargs={}, …)

        Same as :meth:`apply_async` but executed the task inline instead
        of sending a task message.

    .. method:: signature.freeze(_id=None)

        Finalize the signature by adding a concrete task id.
        The task will not be called and you should not call the signature
        twice after freezing it as that will result in two task messages
        using the same task id.

        :returns: :class:`@AsyncResult` instance.

    .. method:: signature.clone(args=(), kwargs={}, …)

        Return a copy of this signature.

        :keyword args: Partial args to be prepended to the existing args.
        :keyword kwargs: Partial kwargs to be merged with the existing kwargs.
        :keyword options: Partial options to be merged with the existing
                          options.

    .. method:: signature.replace(args=None, kwargs=None, options=None)

        Replace the args, kwargs or options set for this signature.
        These are only replaced if the selected is not :const:`None`.

    .. method:: signature.link(other_signature)

        Add a callback task to be applied if this task
        executes successfully.

        :returns: ``other_signature`` (to work with :func:`~functools.reduce`).

    .. method:: signature.link_error(other_signature)

        Add a callback task to be applied if an error occurs
        while executing this task.

        :returns: ``other_signature`` (to work with :func:`~functools.reduce`)

    .. method:: signature.set(…)

        Set arbitrary options (same as ``.options.update(…)``).

        This is a chaining method call (i.e. it will return ``self``).

    .. method:: signature.flatten_links()

        Gives a recursive list of dependencies (unchain if you will,
        but with links intact).

Proxies
-------

.. data:: current_app

    The currently set app for this thread.

.. data:: current_task

    The task currently being executed
    (only set in the worker, or when eager/apply is used).

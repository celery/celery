========
 celery
========

.. currentmodule:: celery

.. module:: celery


.. contents::
    :local:

Application
-----------

.. class:: Celery(main='__main__', broker='amqp://localhost//', ...)

    :param main: Name of the main module if running as `__main__`.
    :keyword broker: URL of the default broker used.
    :keyword loader: The loader class, or the name of the loader class to use.
                     Default is :class:`celery.loaders.app.AppLoader`.
    :keyword backend: The result store backend class, or the name of the
                      backend class to use. Default is the value of the
                      :setting:`CELERY_RESULT_BACKEND` setting.
    :keyword amqp: AMQP object or class name.
    :keyword events: Events object or class name.
    :keyword log: Log object or class name.
    :keyword control: Control object or class name.
    :keyword set_as_current:  Make this the global current app.
    :keyword tasks: A task registry or the name of a registry class.

    .. attribute:: Celery.main

        Name of the `__main__` module.  Required for standalone scripts.

        If set this will be used instead of `__main__` when automatically
        generating task names.

    .. attribute:: Celery.conf

        Current configuration.

    .. attribute:: Celery.current_task

        The instance of the task that is being executed, or :const:`None`.

    .. attribute:: Celery.amqp

        AMQP related functionality: :class:`~@amqp`.

    .. attribute:: Celery.backend

        Current backend instance.

    .. attribute:: Celery.loader

        Current loader instance.

    .. attribute:: Celery.control

        Remote control: :class:`~@control`.

    .. attribute:: Celery.events

        Consuming and sending events: :class:`~@events`.

    .. attribute:: Celery.log

        Logging: :class:`~@log`.

    .. attribute:: Celery.tasks

        Task registry.

        Accessing this attribute will also finalize the app.

    .. attribute:: Celery.pool

        Broker connection pool: :class:`~@pool`.
        This attribute is not related to the workers concurrency pool.

    .. attribute:: Celery.Task

        Base task class for this app.

    .. method:: Celery.close

        Cleans-up after application, like closing any pool connections.
        Only necessary for dynamically created apps for which you can
        use the with statement::

            with Celery(set_as_current=False) as app:
                with app.connection() as conn:
                    pass

    .. method:: Celery.bugreport

        Returns a string with information useful for the Celery core
        developers when reporting a bug.

    .. method:: Celery.config_from_object(obj, silent=False)

        Reads configuration from object, where object is either
        an object or the name of a module to import.

        :keyword silent: If true then import errors will be ignored.

        .. code-block:: python

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

    .. method:: Celery.config_from_envvar(variable_name, silent=False)

        Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

        .. code-block:: python

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

    .. method:: Celery.add_defaults(d)

        Add default configuration from dict ``d``.

        If the argument is a callable function then it will be regarded
        as a promise, and it won't be loaded until the configuration is
        actually needed.

        This method can be compared to::

            >>> celery.conf.update(d)

        with a difference that 1) no copy will be made and 2) the dict will
        not be transferred when the worker spawns child processes, so
        it's important that the same configuration happens at import time
        when pickle restores the object on the other side.

    .. method:: Celery.start(argv=None)

        Run :program:`celery` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.

    .. method:: Celery.task(fun, ...)

        Decorator to create a task class out of any callable.

        Examples:

        .. code-block:: python

            @celery.task
            def refresh_feed(url):
                return ...

        with setting extra options:

        .. code-block:: python

            @celery.task(exchange="feeds")
            def refresh_feed(url):
                return ...

        .. admonition:: App Binding

            For custom apps the task decorator returns proxy
            objects, so that the act of creating the task is not performed
            until the task is used or the task registry is accessed.

            If you are depending on binding to be deferred, then you must
            not access any attributes on the returned object until the
            application is fully set up (finalized).


    .. method:: Celery.send_task(name[, args[, kwargs[, ...]]])

        Send task by name.

        :param name: Name of task to call (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Otherwise supports the same arguments as :meth:`@-Task.apply_async`.

    .. attribute:: Celery.AsyncResult

        Create new result instance. See :class:`~celery.result.AsyncResult`.

    .. attribute:: Celery.GroupResult

        Create new taskset result instance.
        See :class:`~celery.result.GroupResult`.

    .. method:: Celery.worker_main(argv=None)

        Run :program:`celeryd` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified."""

    .. attribute:: Celery.Worker

        Worker application. See :class:`~@Worker`.

    .. attribute:: Celery.WorkController

        Embeddable worker. See :class:`~@WorkController`.

    .. attribute:: Celery.Beat

        Celerybeat scheduler application.
        See :class:`~@Beat`.

    .. method:: Celery.connection(url=default, [ssl, [transport_options={}]])

        Establish a connection to the message broker.

        :param url: Either the URL or the hostname of the broker to use.

        :keyword hostname: URL, Hostname/IP-address of the broker.
            If an URL is used, then the other argument below will
            be taken from the URL instead.
        :keyword userid: Username to authenticate as.
        :keyword password: Password to authenticate with
        :keyword virtual_host: Virtual host to use (domain).
        :keyword port: Port to connect to.
        :keyword ssl: Defaults to the :setting:`BROKER_USE_SSL` setting.
        :keyword transport: defaults to the :setting:`BROKER_TRANSPORT`
                 setting.

        :returns :class:`kombu.connection.Connection`:

    .. method:: Celery.connection_or_acquire(connection=None)

        For use within a with-statement to get a connection from the pool
        if one is not already provided.

        :keyword connection: If not provided, then a connection will be
                             acquired from the connection pool.

    .. method:: Celery.producer_or_acquire(producer=None)

        For use within a with-statement to get a producer from the pool
        if one is not already provided

        :keyword producer: If not provided, then a producer will be
                           acquired from the producer pool.

    .. method:: Celery.mail_admins(subject, body, fail_silently=False)

        Sends an email to the admins in the :setting:`ADMINS` setting.

    .. method:: Celery.select_queues(queues=[])

        Select a subset of queues, where queues must be a list of queue
        names to keep.

    .. method:: Celery.now()

        Returns the current time and date as a :class:`~datetime.datetime`
        object.

    .. method:: Celery.set_current()

        Makes this the current app for this thread.

    .. method:: Celery.finalize()

        Finalizes the app by loading built-in tasks,
        and evaluating pending task decorators

    .. attribute:: Celery.Pickler

        Helper class used to pickle this application.

Grouping Tasks
--------------

.. class:: group(task1[, task2[, task3[,... taskN]]])

    Creates a group of tasks to be executed in parallel.

    Example::

        >>> res = group([add.s(2, 2), add.s(4, 4)]).apply_async()
        >>> res.get()
        [4, 8]

    The ``apply_async`` method returns :class:`~@GroupResult`.

.. class:: chain(task1[, task2[, task3[,... taskN]]])

    Chains tasks together, so that each tasks follows each other
    by being applied as a callback of the previous task.

    If called with only one argument, then that argument must
    be an iterable of tasks to chain.

    Example::

        >>> res = chain(add.s(2, 2), add.s(4)).apply_async()

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

.. class:: subtask(task=None, args=(), kwargs={}, options={})

    Describes the arguments and execution options for a single task invocation.

    Used as the parts in a :class:`group` or to safely pass
    tasks around as callbacks.

    Subtasks can also be created from tasks::

        >>> add.subtask(args=(), kwargs={}, options={})

    or the ``.s()`` shortcut::

        >>> add.s(*args, **kwargs)

    :param task: Either a task class/instance, or the name of a task.
    :keyword args: Positional arguments to apply.
    :keyword kwargs: Keyword arguments to apply.
    :keyword options: Additional options to :meth:`Task.apply_async`.

    Note that if the first argument is a :class:`dict`, the other
    arguments will be ignored and the values in the dict will be used
    instead.

        >>> s = subtask("tasks.add", args=(2, 2))
        >>> subtask(s)
        {"task": "tasks.add", args=(2, 2), kwargs={}, options={}}

    .. method:: subtask.delay(*args, \*\*kwargs)

        Shortcut to :meth:`apply_async`.

    .. method:: subtask.apply_async(args=(), kwargs={}, ...)

        Apply this task asynchronously.

        :keyword args: Partial args to be prepended to the existing args.
        :keyword kwargs: Partial kwargs to be merged with the existing kwargs.
        :keyword options: Partial options to be merged with the existing
                          options.

        See :meth:`~@Task.apply_async`.

    .. method:: subtask.apply(args=(), kwargs={}, ...)

        Same as :meth:`apply_async` but executed the task inline instead
        of sending a task message.

    .. method:: subtask.clone(args=(), kwargs={}, ...)

        Returns a copy of this subtask.

        :keyword args: Partial args to be prepended to the existing args.
        :keyword kwargs: Partial kwargs to be merged with the existing kwargs.
        :keyword options: Partial options to be merged with the existing
                          options.

    .. method:: subtask.replace(args=None, kwargs=None, options=None)

        Replace the args, kwargs or options set for this subtask.
        These are only replaced if the selected is not :const:`None`.

    .. method:: subtask.link(other_subtask)

        Add a callback task to be applied if this task
        executes successfully.

        :returns: ``other_subtask`` (to work with :func:`~functools.reduce`).

    .. method:: subtask.link_error(other_subtask)

        Add a callback task to be applied if an error occurs
        while executing this task.

        :returns: ``other_subtask`` (to work with :func:`~functools.reduce`)

    .. method:: subtask.set(...)

        Set arbitrary options (same as ``.options.update(...)``).

        This is a chaining method call (i.e. it returns itself).

    .. method:: subtask.flatten_links()

        Gives a recursive list of dependencies (unchain if you will,
        but with links intact).

Proxies
-------

.. data:: current_app

    The currently set app for this thread.

.. data:: current_task

    The task currently being executed
    (only set in the worker, or when eager/apply is used).

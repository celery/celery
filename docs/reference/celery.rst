========
 celery
========

.. contents::
    :local:

Application
-----------

.. class:: Celery(main=None, broker="amqp://guest:guest@localhost:5672//",
                  loader="app", backend=None)

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

    .. attribute:: main

        Name of the `__main__` module.  Required for standalone scripts.

        If set this will be used instead of `__main__` when automatically
        generating task names.

    .. attribute:: conf

        Current configuration.

    .. attribute:: current_task

        The instance of the task that is being executed, or :const:`None`.

    .. attribute:: amqp

        AMQP related functionality: :class:`~@amqp`.

    .. attribute:: backend

        Current backend instance.

    .. attribute:: loader

        Current loader instance.

    .. attribute:: control

        Remote control: :class:`~@control`.

    .. attribute:: events

        Consuming and sending events: :class:`~@events`.

    .. attribute:: log

        Logging: :class:`~@log`.

    .. attribute:: tasks

        Task registry.

    .. attribute:: pool

        Broker connection pool: :class:`~@pool`.

    .. attribute:: Task

        Base task class for this app.

    .. method:: bugreport

        Returns a string with information useful for the Celery core
        developers when reporting a bug.

    .. method:: config_from_object(obj, silent=False)

        Reads configuration from object, where object is either
        an object or the name of a module to import.

        :keyword silent: If true then import errors will be ignored.

        .. code-block:: python

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

    .. method:: config_from_envvar(variable_name, silent=False)

        Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

        .. code-block:: python

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

    .. method:: config_from_cmdline(argv, namespace="celery")

        Parses argv for configuration strings.

        Configuration strings must be located after a '--' sequence,
        e.g.::

            program arg1 arg2 -- celeryd.concurrency=10

        :keyword namespace: Default namespace if omitted.

    .. method:: start(argv=None)

        Run :program:`celery` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.

    .. method:: task(fun, **options)

        Decorator to create a task class out of any callable.

        **Examples:**

        .. code-block:: python

            @task
            def refresh_feed(url):
                return ...

        with setting extra options:

        .. code-block:: python

            @task(exchange="feeds")
            def refresh_feed(url):
                return ...

        .. admonition:: App Binding

            For custom apps the task decorator returns proxy
            objects, so that the act of creating the task is not performed
            until the task is used or the task registry is accessed.

            If you are depending on binding to be deferred, then you must
            not access any attributes on the returned object until the
            application is fully set up (finalized).


    .. method:: send_task(name, args=(), kwargs={}, countdown=None,
            eta=None, task_id=None, publisher=None, connection=None,
            result_cls=AsyncResult, expires=None, queues=None, **options)

        Send task by **name**.

        :param name: Name of task to execute (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Otherwise supports the same arguments as :meth:`~@Task.apply_async`.

    .. attribute:: AsyncResult

        Create new result instance. See :class:`~celery.result.AsyncResult`.

    .. attribute:: TaskSetResult

        Create new taskset result instance.
        See :class:`~celery.result.TaskSetResult`.

    .. method:: worker_main(argv=None)

        Run :program:`celeryd` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified."""

    .. attribute:: Worker

        Worker application. See :class:`~@Worker`.

    .. attribute:: WorkController

        Embeddable worker. See :class:`~@WorkController`.

    .. attribute:: Beat

        Celerybeat scheduler application.
        See :class:`~@Beat`.

    .. method:: broker_connection(url="amqp://guest:guest@localhost:5672//",
            ssl=False, transport_options={})

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

        :returns :class:`kombu.connection.BrokerConnection`:

    .. method:: default_connection(connection=None)

        For use within a with-statement to get a connection from the pool
        if one is not already provided.

        :keyword connection: If not provided, then a connection will be
                             acquired from the connection pool.


    .. method:: mail_admins(subject, body, fail_silently=False)

        Sends an email to the admins in the :setting:`ADMINS` setting.

    .. method:: select_queues(queues=[])

        Select a subset of queues, where queues must be a list of queue
        names to keep.

    .. method:: now()

        Returns the current time and date as a :class:`~datetime.datetime`
        object.

    .. method:: set_current()

        Makes this the current app for this thread.

    .. method:: finalize()

        Finalizes the app by loading built-in tasks,
        and evaluating pending task decorators

    .. attribute:: Pickler

        Helper class used to pickle this application.

Grouping Tasks
--------------

.. class:: group(tasks=[])

    Creates a group of tasks to be executed in parallel.

    Example::

        >>> res = group([add.s(2, 2), add.s(4, 4)]).apply_async()
        >>> res.get()
        [4, 8]

    The ``apply_async`` method returns :class:`~@TaskSetResult`.

.. class:: chain(*tasks)

    Chains tasks together, so that each tasks follows each other
    by being applied as a callback of the previous task.

    Example::

        >>> res = chain(add.s(2, 2), add.s(4)).apply_async()

    is effectively :math:`(2 + 2) + 4)`::

        >>> res.get()
        8

    Applying a chain will return the result of the last task in the chain.
    You can get to the other tasks by following the ``result.parent``'s::

        >>> res.parent.get()
        4

.. class:: chord(header)(body)

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

    .. method:: delay(*args, **kwargs)

        Shortcut to :meth:`apply_async`.

    .. method:: apply_async(args=(), kwargs={}, **options)

        Apply this task asynchronously.

        :keyword args: Partial args to be prepended to the existing args.
        :keyword kwargs: Partial kwargs to be merged with the existing kwargs.
        :keyword options: Partial options to be merged with the existing
                          options.

        See :meth:`~@Task.apply_async`.

    .. method:: apply(args=(), kwargs={}, **options)

        Same as :meth:`apply_async` but executes inline instead
        of sending a task message.

    .. method:: clone(args=(), kwargs={}, **options)

        Returns a copy of this subtask.

        :keyword args: Partial args to be prepended to the existing args.
        :keyword kwargs: Partial kwargs to be merged with the existing kwargs.
        :keyword options: Partial options to be merged with the existing
                          options.

    .. method:: replace(args=None, kwargs=None, options=None)

        Replace the args, kwargs or options set for this subtask.
        These are only replaced if the selected is not :const:`None`.

    .. method:: link(other_subtask)

        Add a callback task to be applied if this task
        executes successfully.

    .. method:: link_error(other_subtask)

        Add a callback task to be applied if an error occurs
        while executing this task.

    .. method:: set(**options)

        Set arbitrary options (same as ``.options.update(...)``).

        This is a chaining method call (i.e. it returns itself).

    .. method:: flatten_links()

        Gives a recursive list of dependencies (unchain if you will,
        but with links intact).

Proxies
-------

.. data:: current_app

    The currently set app for this thread.

.. data:: current_task

    The task currently being executed
    (only set in the worker, or when eager/apply is used).

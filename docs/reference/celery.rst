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

.. class:: Celery(main='__main__', broker='amqp://localhost//', …)

    :param main: Name of the main module if running as `__main__`.
        This is used as a prefix for task names.
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
    :keyword include: List of modules every worker should import.
    :keyword fixups: List of fixup plug-ins (see e.g.
        :mod:`celery.fixups.django`).
    :keyword autofinalize: If set to False a :exc:`RuntimeError`
        will be raised if the task registry or tasks are used before
        the app is finalized.

    .. attribute:: Celery.main

        Name of the `__main__` module.  Required for standalone scripts.

        If set this will be used instead of `__main__` when automatically
        generating task names.

    .. attribute:: Celery.conf

        Current configuration.

    .. attribute:: user_options

        Custom options for command-line programs.
        See :ref:`extending-commandoptions`

    .. attribute:: steps

        Custom bootsteps to extend and modify the worker.
        See :ref:`extending-bootsteps`.

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

    .. attribute:: Celery.timezone

        Current timezone for this app.
        This is a cached property taking the time zone from the
        :setting:`CELERY_TIMEZONE` setting.

    .. method:: Celery.close

        Close any open pool connections and do any other steps necessary
        to clean up after the application.

        Only necessary for dynamically created apps for which you can
        use the with statement instead::

            with Celery(set_as_current=False) as app:
                with app.connection() as conn:
                    pass

    .. method:: Celery.signature

        Return a new :class:`~celery.canvas.Signature` bound to this app.
        See :meth:`~celery.signature`

    .. method:: Celery.bugreport

        Return a string with information useful for the Celery core
        developers when reporting a bug.

    .. method:: Celery.config_from_object(obj, silent=False, force=False)

        Reads configuration from object, where object is either
        an object or the name of a module to import.

        :keyword silent: If true then import errors will be ignored.

        :keyword force:  Force reading configuration immediately.
            By default the configuration will be read only when required.

        .. code-block:: python

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

    .. method:: Celery.config_from_envvar(variable_name,
                                          silent=False, force=False)

        Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

        .. code-block:: python

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

    .. method:: Celery.autodiscover_tasks(packages, related_name="tasks")

        With a list of packages, try to import modules of a specific name (by
        default 'tasks').

        For example if you have an (imagined) directory tree like this::

            foo/__init__.py
               tasks.py
               models.py

            bar/__init__.py
                tasks.py
                models.py

            baz/__init__.py
                models.py

        Then calling ``app.autodiscover_tasks(['foo', bar', 'baz'])`` will
        result in the modules ``foo.tasks`` and ``bar.tasks`` being imported.

        :param packages: List of packages to search.
            This argument may also be a callable, in which case the
            value returned is used (for lazy evaluation).

        :keyword related_name: The name of the module to find.  Defaults
            to "tasks", which means it look for "module.tasks" for every
            module in ``packages``.
        :keyword force: By default this call is lazy so that the actual
            autodiscovery will not happen until an application imports the
            default modules.  Forcing will cause the autodiscovery to happen
            immediately.


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

    .. method:: Celery.setup_security(…)

        Setup the message-signing serializer.
        This will affect all application instances (a global operation).

        Disables untrusted serializers and if configured to use the ``auth``
        serializer will register the auth serializer with the provided settings
        into the Kombu serializer registry.

        :keyword allowed_serializers:  List of serializer names, or content_types
            that should be exempt from being disabled.
        :keyword key: Name of private key file to use.
            Defaults to the :setting:`CELERY_SECURITY_KEY` setting.
        :keyword cert: Name of certificate file to use.
            Defaults to the :setting:`CELERY_SECURITY_CERTIFICATE` setting.
        :keyword store: Directory containing certificates.
            Defaults to the :setting:`CELERY_SECURITY_CERT_STORE` setting.
        :keyword digest: Digest algorithm used when signing messages.
            Default is ``sha1``.
        :keyword serializer: Serializer used to encode messages after
            they have been signed.  See :setting:`CELERY_TASK_SERIALIZER` for
            the serializers supported.
            Default is ``json``.

    .. method:: Celery.start(argv=None)

        Run :program:`celery` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.

    .. method:: Celery.task(fun, …)

        Decorator to create a task class out of any callable.

        Examples:

        .. code-block:: python

            @app.task
            def refresh_feed(url):
                return …

        with setting extra options:

        .. code-block:: python

            @app.task(exchange="feeds")
            def refresh_feed(url):
                return …

        .. admonition:: App Binding

            For custom apps the task decorator will return a proxy
            object, so that the act of creating the task is not performed
            until the task is used or the task registry is accessed.

            If you are depending on binding to be deferred, then you must
            not access any attributes on the returned object until the
            application is fully set up (finalized).


    .. method:: Celery.send_task(name[, args[, kwargs[, …]]])

        Send task by name.

        :param name: Name of task to call (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Otherwise supports the same arguments as :meth:`@-Task.apply_async`.

    .. attribute:: Celery.AsyncResult

        Create new result instance. See :class:`~celery.result.AsyncResult`.

    .. attribute:: Celery.GroupResult

        Create new group result instance.
        See :class:`~celery.result.GroupResult`.

    .. method:: Celery.worker_main(argv=None)

        Run :program:`celery worker` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.

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

        :returns :class:`kombu.Connection`:

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

        Return the current time and date as a :class:`~datetime.datetime`
        object.

    .. method:: Celery.set_current()

        Makes this the current app for this thread.

    .. method:: Celery.finalize()

        Finalizes the app by loading built-in tasks,
        and evaluating pending task decorators

    .. attribute:: Celery.Pickler

        Helper class used to pickle this application.

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

    Will return a `group` task that when called will then call of the
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

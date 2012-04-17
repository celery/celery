.. currentmodule:: celery

.. automodule:: celery

    .. contents::
        :local:

    Application
    -----------

    .. autoclass:: Celery

        .. attribute:: main

            Name of the `__main__` module.  Required for standalone scripts.

            If set this will be used instead of `__main__` when automatically
            generating task names.

        .. autoattribute:: conf
        .. autoattribute:: amqp
        .. autoattribute:: backend
        .. autoattribute:: loader
        .. autoattribute:: control
        .. autoattribute:: events
        .. autoattribute:: log
        .. autoattribute:: tasks
        .. autoattribute:: pool
        .. autoattribute:: Task

        .. automethod:: bugreport

        .. automethod:: config_from_object
        .. automethod:: config_from_envvar
        .. automethod:: config_from_cmdline

        .. automethod:: start

        .. automethod:: task
        .. automethod:: send_task
        .. autoattribute:: AsyncResult
        .. autoattribute:: TaskSetResult

        .. automethod:: worker_main
        .. autoattribute:: Worker
        .. autoattribute:: WorkController
        .. autoattribute:: Beat

        .. automethod:: broker_connection
        .. automethod:: default_connection

        .. automethod:: mail_admins

        .. automethod:: prepare_config
        .. automethod:: select_queues
        .. automethod:: now

        .. automethod:: set_current
        .. automethod:: finalize

        .. autoattribute:: Pickler

    Grouping Tasks
    --------------

    .. autofunction:: group

    .. autofunction:: chain

    .. autofunction:: chord

    .. autofunction:: subtask

    Proxies
    -------

    .. data:: current_app

        The currently set app for this thread.

    .. data:: current_task

        The task currently being executed
        (only set in the worker, or when eager/apply is used).


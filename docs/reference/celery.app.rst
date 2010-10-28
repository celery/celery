.. currentmodule:: celery.app

.. automodule:: celery.app

    .. contents::
        :local:

    Functions
    ---------

    .. autofunction:: app_or_default

    Application
    -----------

    .. autoclass:: App

        .. attribute:: main

            Name of the `__main__` module.  Required for standalone scripts.

            If set this will be used instead of `__main__` when automatically
            generating task names.

        .. autoattribute:: amqp
        .. autoattribute:: backend
        .. autoattribute:: loader
        .. autoattribute:: conf
        .. autoattribute:: control
        .. autoattribute:: log

        .. automethod:: config_from_object
        .. automethod:: config_from_envvar
        .. automethod:: config_from_cmdline

        .. automethod:: task
        .. automethod:: create_task_cls
        .. automethod:: TaskSet
        .. automethod:: send_task
        .. automethod:: AsyncResult
        .. automethod:: TaskSetResult

        .. automethod:: worker_main
        .. automethod:: Worker
        .. automethod:: Beat

        .. automethod:: broker_connection
        .. automethod:: with_default_connection

        .. automethod:: mail_admins

        .. automethod:: pre_config_merge
        .. automethod:: post_config_merge

        .. automethod:: either
        .. automethod:: merge

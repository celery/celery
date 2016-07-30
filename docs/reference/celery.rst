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
:class:`Celery`       Celery application instance
:class:`group`        group tasks together
:class:`chain`        chain tasks together
:class:`chord`        chords enable callbacks for groups
:func:`signature`     create a new task signature
:class:`Signature`    object describing a task invocation
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

    .. autoattribute:: current_worker_task

    .. autoattribute:: amqp

    .. autoattribute:: backend

    .. autoattribute:: loader

    .. autoattribute:: control
    .. autoattribute:: events
    .. autoattribute:: log
    .. autoattribute:: tasks
    .. autoattribute:: pool
    .. autoattribute:: producer_pool
    .. autoattribute:: Task
    .. autoattribute:: timezone
    .. autoattribute:: builtin_fixups
    .. autoattribute:: oid

    .. automethod:: close

    .. automethod:: signature

    .. automethod:: bugreport

    .. automethod:: config_from_object

    .. automethod:: config_from_envvar

    .. automethod:: autodiscover_tasks

    .. automethod:: add_defaults

    .. automethod:: add_periodic_task

    .. automethod:: setup_security

    .. automethod:: start

    .. automethod:: task

    .. automethod:: send_task

    .. automethod:: gen_task_name

    .. autoattribute:: AsyncResult

    .. autoattribute:: GroupResult

    .. automethod:: worker_main

    .. autoattribute:: Worker

    .. autoattribute:: WorkController

    .. autoattribute:: Beat

    .. automethod:: connection_for_read

    .. automethod:: connection_for_write

    .. automethod:: connection

    .. automethod:: connection_or_acquire

    .. automethod:: producer_or_acquire

    .. automethod:: select_queues

    .. automethod:: now

    .. automethod:: set_current

    .. automethod:: set_default

    .. automethod:: finalize

    .. automethod:: on_init

    .. automethod:: prepare_config

    .. data:: on_configure

        Signal sent when app is loading configuration.

    .. data:: on_after_configure

        Signal sent after app has prepared the configuration.

    .. data:: on_after_finalize

        Signal sent after app has been finalized.

    .. data:: on_after_fork

        Signal sent in child process after fork.

Canvas primitives
-----------------

See :ref:`guide-canvas` for more about creating task work-flows.

.. autoclass:: group

.. autoclass:: chain

.. autoclass:: chord

.. autofunction:: signature

.. autoclass:: Signature

Proxies
-------

.. data:: current_app

    The currently set app for this thread.

.. data:: current_task

    The task currently being executed
    (only set in the worker, or when eager/apply is used).

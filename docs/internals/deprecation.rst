.. _deprecation-timeline:

==============================
 Celery Deprecation Time-line
==============================

.. contents::
    :local:

.. _deprecations-v5.0:

Removals for version 5.0
========================

Old Task API
------------

.. _deprecate-compat-task-modules:

Compat Task Modules
~~~~~~~~~~~~~~~~~~~

- Module ``celery.decorators`` will be removed:

    This means you need to change:

    .. code-block:: python

        from celery.decorators import task

    Into:

    .. code-block:: python

        from celery import task

- Module ``celery.task`` *may* be removed (not decided)

    This means you should change:

    .. code-block:: python

        from celery.task import task

    into:

    .. code-block:: python

        from celery import task

    -- and:

    .. code-block:: python

        from celery.task import Task

    into:

    .. code-block:: python

        from celery import Task


Note that the new :class:`~celery.Task` class no longer
uses :func:`classmethod` for these methods:

    - delay
    - apply_async
    - retry
    - apply
    - AsyncResult
    - subtask

This also means that you can't call these methods directly
on the class, but have to instantiate the task first:

.. code-block:: pycon

    >>> MyTask.delay()          # NO LONGER WORKS


    >>> MyTask().delay()        # WORKS!


Task attributes
---------------

The task attributes:

- ``queue``
- ``exchange``
- ``exchange_type``
- ``routing_key``
- ``delivery_mode``
- ``priority``

is deprecated and must be set by :setting:`task_routes` instead.


Modules to Remove
-----------------

- ``celery.execute``

  This module only contains ``send_task``: this must be replaced with
  :attr:`@send_task` instead.

- ``celery.decorators``

    See :ref:`deprecate-compat-task-modules`

- ``celery.log``

    Use :attr:`@log` instead.

- ``celery.messaging``

    Use :attr:`@amqp` instead.

- ``celery.registry``

    Use :mod:`celery.app.registry` instead.

- ``celery.task.control``

    Use :attr:`@control` instead.

- ``celery.task.schedules``

    Use :mod:`celery.schedules` instead.

- ``celery.task.chords``

    Use :func:`celery.chord` instead.

Settings
--------

``BROKER`` Settings
~~~~~~~~~~~~~~~~~~~

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
``BROKER_HOST``                        :setting:`broker_url`
``BROKER_PORT``                        :setting:`broker_url`
``BROKER_USER``                        :setting:`broker_url`
``BROKER_PASSWORD``                    :setting:`broker_url`
``BROKER_VHOST``                       :setting:`broker_url`
=====================================  =====================================

``REDIS`` Result Backend Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
``CELERY_REDIS_HOST``                  :setting:`result_backend`
``CELERY_REDIS_PORT``                  :setting:`result_backend`
``CELERY_REDIS_DB``                    :setting:`result_backend`
``CELERY_REDIS_PASSWORD``              :setting:`result_backend`
``REDIS_HOST``                         :setting:`result_backend`
``REDIS_PORT``                         :setting:`result_backend`
``REDIS_DB``                           :setting:`result_backend`
``REDIS_PASSWORD``                     :setting:`result_backend`
=====================================  =====================================


Task_sent signal
----------------

The :signal:`task_sent` signal will be removed in version 4.0.
Please use the :signal:`before_task_publish` and :signal:`after_task_publish`
signals instead.

Result
------

Apply to: :class:`~celery.result.AsyncResult`,
:class:`~celery.result.EagerResult`:

- ``Result.wait()`` -> ``Result.get()``

- ``Result.task_id()`` -> ``Result.id``

- ``Result.status`` -> ``Result.state``.

.. _deprecations-v3.1:


Settings
~~~~~~~~

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
``CELERY_AMQP_TASK_RESULT_EXPIRES``    :setting:`result_expires`
=====================================  =====================================



.. _deprecations-v2.0:

Removals for version 2.0
========================

* The following settings will be removed:

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
`CELERY_AMQP_CONSUMER_QUEUES`          `task_queues`
`CELERY_AMQP_CONSUMER_QUEUES`          `task_queues`
`CELERY_AMQP_EXCHANGE`                 `task_default_exchange`
`CELERY_AMQP_EXCHANGE_TYPE`            `task_default_exchange_type`
`CELERY_AMQP_CONSUMER_ROUTING_KEY`     `task_queues`
`CELERY_AMQP_PUBLISHER_ROUTING_KEY`    `task_default_routing_key`
=====================================  =====================================

* :envvar:`CELERY_LOADER` definitions without class name.

    For example,, `celery.loaders.default`, needs to include the class name:
    `celery.loaders.default.Loader`.

* :meth:`TaskSet.run`. Use :meth:`celery.task.base.TaskSet.apply_async`
    instead.

.. _deprecation-timeline:

=============================
 Celery Deprecation Timeline
=============================

.. contents::
    :local:

.. _deprecations-v3.0:

Removals for version 3.0
========================

Old Task API
------------

.. _deprecate-compat-task-modules:

Compat Task Modules
~~~~~~~~~~~~~~~~~~~

- Module ``celery.decorators`` will be removed:

  Which means you need to change::

    from celery.decorators import task

Into::

    from celery import task

- Module ``celery.task`` *may* be removed (not decided)

    This means you should change::

        from celery.task import task

    into::

        from celery import task

    -- and::

        from celery.task import Task

    into::

        from celery import Task


Note that the new :class:`~celery.Task` class no longer
uses classmethods for these methods:

    - delay
    - apply_async
    - retry
    - apply
    - AsyncResult
    - subtask

This also means that you can't call these methods directly
on the class, but have to instantiate the task first::

    >>> MyTask.delay()          # NO LONGER WORKS


    >>> MyTask().delay()        # WORKS!


Magic keyword arguments
~~~~~~~~~~~~~~~~~~~~~~~

The magic keyword arguments accepted by tasks will be removed
in 3.0, so you should start rewriting any tasks
using the ``celery.decorators`` module and depending
on keyword arguments being passed to the task,
for example::

    from celery.decorators import task

    @task
    def add(x, y, task_id=None):
        print("My task id is %r" % (task_id, ))

must be rewritten into::

    from celery import task

    @task
    def add(x, y):
        print("My task id is %r" % (add.request.id, ))


Task attributes
---------------

The task attributes:

- ``queue``
- ``exchange``
- ``exchange_type``
- ``routing_key``
- ``delivery_mode``
- ``priority``

is deprecated and must be set by :setting:`CELERY_ROUTES` instead.

:mod:`celery.result`
--------------------

- ``BaseAsyncResult`` -> ``AsyncResult``.

- ``TaskSetResult.total`` -> ``len(TaskSetResult)``

Apply to: :class:`~celery.result.AsyncResult`,
:class:`~celery.result.ResultSet`, :class:`~celery.result.EagerResult`,
:class:`~celery.result.TaskSetResult`.

- ``Result.wait()`` -> ``Result.get()``

- ``Result.task_id()`` -> ``Result.id``

- ``TaskSetResult.taskset_id`` -> ``TaskSetResult.id``

- ``Result.status`` -> ``Result.state``.

:mod:`celery.loader`
--------------------

- ``current_loader()`` -> ``current_app.loader``

- ``load_settings()`` -> ``current_app.conf``


Modules to Remove
-----------------

- ``celery.execute``

  This module only contains ``send_task``, which must be replaced with
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
    ``BROKER_HOST``                        :setting:`BROKER_URL`
    ``BROKER_PORT``                        :setting:`BROKER_URL`
    ``BROKER_USER``                        :setting:`BROKER_URL`
    ``BROKER_PASSWORD``                    :setting:`BROKER_URL`
    ``BROKER_VHOST``                       :setting:`BROKER_URL`
    ``BROKER_INSIST``                      *no alternative*

``REDIS`` Result Backend Settings
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    =====================================  =====================================
    **Setting name**                       **Replace with**
    =====================================  =====================================
    ``CELERY_REDIS_HOST``                  :setting:`CELERY_RESULT_BACKEND`
    ``CELERY_REDIS_PORT``                  :setting:`CELERY_RESULT_BACKEND`
    ``CELERY_REDIS_DB``                    :setting:`CELERY_RESULT_BACKEND`
    ``CELERY_REDIS_PASSWORD``              :setting:`CELERY_RESULT_BACKEND`
    ``REDIS_HOST``                         :setting:`CELERY_RESULT_BACKEND`
    ``REDIS_PORT``                         :setting:`CELERY_RESULT_BACKEND`
    ``REDIS_DB``                           :setting:`CELERY_RESULT_BACKEND`
    ``REDIS_PASSWORD``                     :setting:`CELERY_RESULT_BACKEND`

Logging Settings
~~~~~~~~~~~~~~~~

    =====================================  =====================================
    **Setting name**                       **Replace with**
    =====================================  =====================================
    ``CELERYD_LOG_LEVEL``                  :option:`--loglevel`
    ``CELERYD_LOG_FILE``                   :option:`--logfile``
    ``CELERYBEAT_LOG_LEVEL``               :option:`--loglevel`
    ``CELERYBEAT_LOG_FILE``                :option:`--loglevel``
    ``CELERYMON_LOG_LEVEL``                :option:`--loglevel`
    ``CELERYMON_LOG_FILE``                 :option:`--loglevel``

Other Settings
~~~~~~~~~~~~~~

    =====================================  =====================================
    **Setting name**                       **Replace with**
    =====================================  =====================================
    ``CELERY_TASK_ERROR_WITELIST``         Annotate ``Task.ErrorMail``
    ``CELERY_AMQP_TASK_RESULT_EXPIRES``    :setting:`CELERY_TASK_RESULT_EXPIRES`


.. _deprecations-v2.0:

Removals for version 2.0
========================

* The following settings will be removed:

    =====================================  =====================================
    **Setting name**                       **Replace with**
    =====================================  =====================================
    `CELERY_AMQP_CONSUMER_QUEUES`          `CELERY_QUEUES`
    `CELERY_AMQP_CONSUMER_QUEUES`          `CELERY_QUEUES`
    `CELERY_AMQP_EXCHANGE`                 `CELERY_DEFAULT_EXCHANGE`
    `CELERY_AMQP_EXCHANGE_TYPE`            `CELERY_DEFAULT_AMQP_EXCHANGE_TYPE`
    `CELERY_AMQP_CONSUMER_ROUTING_KEY`     `CELERY_QUEUES`
    `CELERY_AMQP_PUBLISHER_ROUTING_KEY`    `CELERY_DEFAULT_ROUTING_KEY`
    =====================================  =====================================

* :envvar:`CELERY_LOADER` definitions without class name.

    E.g. `celery.loaders.default`, needs to include the class name:
    `celery.loaders.default.Loader`.

* :meth:`TaskSet.run`. Use :meth:`celery.task.base.TaskSet.apply_async`
    instead.

* The module :mod:`celery.task.rest`; use :mod:`celery.task.http` instead.

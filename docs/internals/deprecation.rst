.. _deprecation-timeline:

=============================
 Celery Deprecation Timeline
=============================

.. contents::
    :local:

.. _deprecations-v3.2:

Removals for version 3.2
========================

- Module ``celery.task.trace`` has been renamed to ``celery.app.trace``
  as the ``celery.task`` package is being phased out.  The compat module
  will be removed in version 3.2 so please change any import from::

    from celery.task.trace import …

  to::

    from celery.app.trace import …

- ``AsyncResult.serializable()`` and ``celery.result.from_serializable``
  will be removed.

    Use instead::

        >>> tup = result.as_tuple()
        >>> from celery.result import result_from_tuple
        >>> result = result_from_tuple(tup)

.. _deprecations-v4.0:

Removals for version 4.0
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


TaskSet
~~~~~~~

TaskSet has been renamed to group and TaskSet will be removed in version 4.0.

Old::

    >>> from celery.task import TaskSet

    >>> TaskSet(add.subtask((i, i)) for i in xrange(10)).apply_async()

New::

    >>> from celery import group
    >>> group(add.s(i, i) for i in xrange(10))()


Magic keyword arguments
~~~~~~~~~~~~~~~~~~~~~~~

The magic keyword arguments accepted by tasks will be removed
in 4.0, so you should start rewriting any tasks
using the ``celery.decorators`` module and depending
on keyword arguments being passed to the task,
for example::

    from celery.decorators import task

    @task()
    def add(x, y, task_id=None):
        print("My task id is %r" % (task_id, ))

should be rewritten into::

    from celery import task

    @task(bind=True)
    def add(self, x, y):
        print("My task id is {0.request.id}".format(self))


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

- ``TaskSetResult`` -> ``GroupResult``.

- ``TaskSetResult.total`` -> ``len(GroupResult)``

- ``TaskSetResult.taskset_id`` -> ``GroupResult.id``

Apply to: :class:`~celery.result.AsyncResult`,
:class:`~celery.result.EagerResult`::

- ``Result.wait()`` -> ``Result.get()``

- ``Result.task_id()`` -> ``Result.id``

- ``Result.status`` -> ``Result.state``.

:mod:`celery.loader`
--------------------

- ``current_loader()`` -> ``current_app.loader``

- ``load_settings()`` -> ``current_app.conf``


Task_sent signal
----------------

The :signal:`task_sent` signal will be removed in version 4.0.
Please use the :signal:`before_task_publish` and :signal:`after_task_publush`
signals instead.


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
=====================================  =====================================


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
=====================================  =====================================

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
=====================================  =====================================

Other Settings
~~~~~~~~~~~~~~

=====================================  =====================================
**Setting name**                       **Replace with**
=====================================  =====================================
``CELERY_TASK_ERROR_WITELIST``         Annotate ``Task.ErrorMail``
``CELERY_AMQP_TASK_RESULT_EXPIRES``    :setting:`CELERY_TASK_RESULT_EXPIRES`
=====================================  =====================================


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

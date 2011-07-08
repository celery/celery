.. _deprecation-timeline:

=============================
 Celery Deprecation Timeline
=============================

.. contents::
    :local:

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

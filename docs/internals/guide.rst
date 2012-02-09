.. _internals-guide:

================================
 Contributors Guide to the Code
================================

.. contents::
    :local:

Applications vs. "single mode"
==============================

In the beginning Celery was developed for Django, simply because
this enabled us get the project started quickly, while also having
a large potential user base.

In Django there is a global settings object, so multiple Django projects
can't co-exist in the same process space, this later posed a problem
for using Celery with frameworks that doesn't have this limitation.

Therefore the app concept was introduced.  When using apps you use 'celery'
objects instead of importing things from celery submodules, this sadly
also means that Celery essentially has two APIs.

Here's an example using Celery in single-mode:

.. code-block:: python

    from celery.task import task
    from celery.task.control import inspect

    from .models import CeleryStats

    @task
    def write_stats_to_db():
        stats = inspect().stats(timeout=1)
        for node_name, reply in stats:
            CeleryStats.objects.update_stat(node_name, stats)


and here's the same using Celery app objects:

.. code-block:: python

    from .celery import celery
    from .models import CeleryStats

    @celery.task
    def write_stats_to_db():
        stats = celery.control.inspect().stats(timeout=1)
        for node_name, reply in stats:
            CeleryStats.objects.update_stat(node_name, stats)


In the example above the actual application instance is imported
from a module in the project, this module could look something like this:

.. code-block:: python

    from celery import Celery

    celery = Celery()
    celery.config_from_object(BROKER_URL="amqp://")


Module Overview
===============

- celery.app

    This is the core of Celery: the entry-point for all functionality.

- celery.loaders

    Every app must have a loader.  The loader decides how configuration
    is read, what happens when the worker starts, when a task starts and ends,
    and so on.

    The loaders included are:

        - app

            Custom celery app instances uses this loader by default.

        - default

            "single-mode" uses this loader by default.

    Extension loaders also exist, like ``django-celery``, ``celery-pylons``
    and so on.

- celery.worker

    This is the worker implementation.

- celery.backends

    Task result backends live here.

- celery.apps

    Major user applications: ``celeryd``, and ``celerybeat``
- celery.bin

    Command line applications.
    setup.py creates setuptools entrypoints for these.

- celery.concurrency

    Execution pool implementations (processes, eventlet, gevent, threads).

- celery.db

    Database models for the SQLAlchemy database result backend.
    (should be moved into :mod:`celery.backends.database`)

- celery.events

    Sending and consuming monitoring events, also includes curses monitor,
    event dumper and utilities to work with in-memory cluster state.

- celery.execute.trace

    How tasks are executed and traced by the worker, and in eager mode.

- celery.security

    Security related functionality, currently a serializer using
    cryptographic digests.

- celery.task

    single-mode interface to creating tasks, and controlling workers.

- celery.tests

    The celery unittest suite.

- celery.utils

    Utility functions used by the celery code base.
    Much of it is there to be compatible across Python versions.

- celery.contrib

    Additional public code that doesn't fit into any other namespace.


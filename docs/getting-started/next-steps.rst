.. _next-steps:

============
 Next Steps
============

The :ref:`first-steps` guide is intentionally minimal.  In this guide
we will demonstrate what Celery offers in more detail, including
how to add Celery support for your application and library.

.. contents::
    :local:
    :depth: 1

Using Celery in your Application
================================

.. _project-layout:

Our Project
-----------

Project layout::

    proj/__init__.py
        /celery.py
        /tasks.py

:file:`proj/celery.py`
~~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/next-steps/proj/celery.py
    :language: python

In this module we created our :class:`@Celery` instance (sometimes
referred to as the *app*).  To use Celery within your project
you simply import this instance.

- The ``broker`` argument specifies the URL of the broker to use.

    See :ref:`celerytut-broker` for more information.

- The ``backend`` argument specifies the result backend to use,

    It's used to keep track of task state and results.
    While results are disabled by default we use the amqp backend here
    to demonstrate how retrieving the results work, you may want to use
    a different backend for your application, as they all have different
    strenghts and weaknesses.  If you don't need results it's best
    to disable them.  Results can also be disabled for individual tasks
    by setting the ``@task(ignore_result=True)`` option.

    See :ref:`celerytut-keeping-results` for more information.

- The ``include`` argument is a list of modules to import when
  the worker starts.  We need to add our tasks module here so
  that the worker is able to find our tasks.

:file:`proj/tasks.py`
~~~~~~~~~~~~~~~~~~~~~

.. literalinclude:: ../../examples/next-steps/proj/tasks.py
    :language: python


Starting the worker
-------------------

The :program:`celery` program can be used to start the worker::

    $ celery worker --app=proj -l info

When the worker starts you should see a banner and some messages::

     -------------- celery@halcyon.local v2.6.0rc4
     ---- **** -----
     --- * ***  * -- [Configuration]
     -- * - **** --- . broker:      amqp://guest@localhost:5672//
     - ** ---------- . app:         __main__:0x1012d8590
     - ** ---------- . concurrency: 8 (processes)
     - ** ---------- . events:      OFF (enable -E to monitor this worker)
     - ** ----------
     - *** --- * --- [Queues]
     -- ******* ---- . celery:      exchange:celery(direct) binding:celery
     --- ***** -----

     [2012-06-08 16:23:51,078: WARNING/MainProcess] celery@halcyon.local has started.

-- The *broker* is the URL you specifed in the broker argument in our ``celery``
module, you can also specify a different broker on the command line by using
the :option:`-b` option.

-- *Concurrency* is the number of multiprocessing worker process used
to process your tasks concurrently, when all of these are busy doing work
new tasks will have to wait for one of the tasks to finish before
it can be processed.

The default concurrency number is the number of CPU's on that machine
(including cores), you can specify a custom number using :option:`-c` option.
There is no recommended value, as the optimal number depends on a number of
factors, but if your tasks are mostly I/O-bound then you can try to increase
it, experimentation has shown that adding more than twice the number
of CPU's is rarely effective, and likely to degrade performance
instead.

Including the default multiprocessing pool, Celery also supports using
Eventlet, Gevent, and threads (see :ref:`concurrency`).

-- *Events* is an option that when enabled causes Celery to send
monitoring messages (events) for actions occurring in the worker.
These can be used by monitor programs like ``celery events``,
celerymon and the Django-Celery admin monitor that you can read
about in the :ref:`Monitoring and Management guide <guide-monitoring>`.

-- *Queues* is the list of queues that the worker will consume
tasks from.  The worker can be told to consume from several queues
at once, and this is used to route messages to specific workers
as a means for Quality of Service, separation of concerns,
and emulating priorities, all described in the :ref:`Routing Guide
<guide-routing>`.

You can get a complete list of command line arguments
by passing in the `--help` flag::

    $ celery worker --help

These options are described in more detailed in the :ref:`Workers Guide <guide-workers>`.

.. sidebar:: About the :option:`--app` argument

    The :option:`--app` argument specifies the Celery app instance to use,
    it must be in the form of ``module.path:celery``, where the part before the colon
    is the name of the module, and the attribute name comes last.
    If a package name is specified instead it will automatically
    try to find a ``celery`` module in that package, and if the name
    is a module it will try to find a ``celery`` attribute in that module.
    This means that these are all equal:

        $ celery --app=proj
        $ celery --app=proj.celery:
        $ celery --app=proj.celery:celery


.. _designing-workflows:

*Canvas*: Designing Workflows
=============================

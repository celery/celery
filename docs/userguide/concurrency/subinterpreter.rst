.. _concurrency-subinterpreter:

==================================
 Concurrency with subinterpreters
==================================

.. _subinterpreter-introduction:

Introduction
============

Python 3.14 added :mod:`concurrent.interpreters` (:pep:`734`): isolated
interpreters that run in the same process, each with its own GIL.  The
``subinterpreter`` pool runs tasks in a fixed set of them, so CPU-bound
tasks run in parallel like with ``prefork``, with the lower memory and
startup cost of threads.

Each subinterpreter is set up once when the worker starts, like a ``prefork``
child process started with ``spawn``: it imports the task modules, sets up
logging and sends the :signal:`worker_process_init` signal.  Tasks then run
in whichever subinterpreter is free, while acknowledging messages and
reporting results is done by the worker's main interpreter.

Enabling the subinterpreter pool
================================

The pool is available on Python 3.14 and later.  Enable it with the
:option:`celery worker -P subinterpreter` or
:option:`celery worker --pool=subinterpreter` worker option:

.. code-block:: console

    $ celery -A proj worker -P subinterpreter -c 4

The concurrency is the number of subinterpreters, and defaults to the number
of CPUs available to the process.

Limitations
===========

Extension modules
-----------------

An extension module can only be loaded in a subinterpreter if it declares
support for it.  Many do not yet, for example ``pydantic-core``, ``gevent``
or ``psutil``, and importing one fails with:

.. code-block:: text

    ImportError: module pydantic_core._pydantic_core does not support loading in subinterpreters

Every subinterpreter imports the task modules when the worker starts, so a
task module that imports such a module (directly or not) stops the worker
at startup.  Use another pool in that case.

Isolation
---------

Subinterpreters share the process but no Python objects: module-level state
(caches, connection pools, global variables) is separate in each
subinterpreter, as it is in each ``prefork`` child process.  Task arguments
and return values are pickled to cross between interpreters.

Time limits and terminating tasks
---------------------------------

A running subinterpreter cannot be interrupted, so:

- :ref:`task time limits <worker-time-limits>` are not supported and are
  ignored, with a warning at startup;
- a task revoked with ``terminate=True`` runs to completion;
- on :ref:`cold shutdown <worker-cold-shutdown>` the running tasks run to
  completion before the worker exits, as on warm shutdown.  Tasks that did
  not start yet are not run.

:setting:`worker_max_tasks_per_child` and
:setting:`worker_max_memory_per_child` are not supported either.

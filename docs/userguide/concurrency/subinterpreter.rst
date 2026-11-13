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
logging and sends the :signal:`worker_process_init` signal (and
:signal:`worker_process_shutdown` when it is closed).  Tasks then run
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

Stopping running tasks
----------------------

A running subinterpreter cannot be interrupted, so the pool cannot stop a
task by itself.  Instead, it *asks* the task to stop when:

- the task is revoked with ``terminate=True``;
- the worker does a :ref:`cold shutdown <worker-cold-shutdown>`;
- the task exceeds its soft time limit.

A task sees the request with
:func:`~celery.concurrency.subinterpreter.stop_requested`, and should check
it regularly if it runs for long:

.. code-block:: python

    from celery.concurrency.subinterpreter import stop_requested
    from celery.exceptions import Ignore

    @app.task
    def process(items):
        for item in items:
            if stop_requested():
                raise Ignore()
            handle(item)

A task that does not check it runs to completion, and the worker waits for
it before exiting, even on cold shutdown.

What the task does when it stops is recorded as usual: returning normally
records a success, even after the task was revoked, and raising an exception
records a failure.  Raising :exc:`~celery.exceptions.Ignore` records nothing,
so a revoked task stays ``REVOKED``.

:func:`~celery.concurrency.subinterpreter.stop_requested` always returns
false outside of the subinterpreter pool, but importing it needs
Python 3.14.

Hard :ref:`time limits <worker-time-limits>` are not supported, and are
ignored with a warning at startup.

Recycling subinterpreters
-------------------------

With :setting:`worker_max_tasks_per_child`, a subinterpreter is replaced by a
new one after running that many tasks, which releases what the tasks leaked
in it.  The :signal:`worker_process_shutdown` signal is sent in a
subinterpreter before it is closed.

:setting:`worker_max_memory_per_child` is not supported: memory is only
measured for the whole process.

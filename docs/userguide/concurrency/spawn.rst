.. _concurrency-spawn:

=======================
 Spawn Start Method
=======================

Celery can use Python's ``spawn`` start method to create worker processes.
This is useful when libraries used by your tasks are not fork-safe, for
example when working with CUDA.

Enable this pool using the :option:`celery worker -P` option:

.. code-block:: console

    $ celery -A proj worker -P spawn -c 4

When using ``spawn`` each worker starts in a fresh Python interpreter
so any global state must be initialized in the child process.

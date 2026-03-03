.. _concurrency:

=============
 Concurrency
=============

:Release: |version|
:Date: |today|

Concurrency in Celery enables the parallel execution of tasks. The default
model, `prefork`, is well-suited for many scenarios and generally recommended
for most users.  In fact, switching to another mode will silently disable
certain features like `soft_timeout` and `max_tasks_per_child`.

This page gives a quick overview of the available options which you can pick
between using the `--pool` option when starting the worker.

Overview of Concurrency Options
-------------------------------

- `prefork`: The default option, ideal for CPU-bound tasks and most use cases.
  It is robust and recommended unless there's a specific need for another model.
- `eventlet` and `gevent`: Designed for IO-bound tasks, these models use
  greenlets for high concurrency. Note that certain features, like `soft_timeout`,
  are not available in these modes.  These have detailed documentation pages
  linked below.
- `solo`: Executes tasks sequentially in the main thread.
- `threads`: Utilizes threading for concurrency, available if the
  `concurrent.futures` module is present.
- `custom`: Enables specifying a custom worker pool implementation through
  environment variables.

.. toctree::
    :maxdepth: 2

    eventlet
    gevent

.. note::
    While alternative models like `eventlet` and `gevent` are available, they
    may lack certain features compared to `prefork`. We recommend `prefork` as
    the starting point unless specific requirements dictate otherwise.

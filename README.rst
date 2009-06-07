============================================
celery - Distributed Task Queue for Django.
============================================

:Version: 0.2.10

Introduction
============

``celery`` is a distributed task queue framework for Django.

It is used for executing tasks *asynchronously*, routed to one or more
worker servers, running concurrently using multiprocessing.

It is designed to solve certain problems related to running websites
demanding high-availability and performance.

It is perfect for filling caches, posting updates to twitter, mass
downloading data like syndication feeds or web scraping. Use-cases are
plentiful. Implementing these features asynchronously using ``celery`` is
easy and fun, and the performance improvements can make it more than
worthwhile.

Features
========

    * Uses AMQP messaging (RabbitMQ, ZeroMQ) to route tasks to the
      worker servers.

    * You can run as many worker servers as you want, and still
      be *guaranteed that the task is only executed once.*

    * Tasks are executed *concurrently* using the Python 2.6
      ``multiprocessing`` module (also available as a back-port
      to older python versions)

    * Supports *periodic tasks*, which makes it a (better) replacement
      for cronjobs.

    * When a task has been executed, the return value is stored using either
      a MySQL/Oracle/PostgreSQL/SQLite database, memcached,
      or Tokyo Tyrant back-end.

    * If the task raises an exception, the exception instance is stored,
      instead of the return value.

    * All tasks has a Universally Unique Identifier (UUID), which is the
      task id, used for querying task status and return values.

    * Supports *task-sets*, which is a task consisting of several sub-tasks.
      You can find out how many, or if all of the sub-tasks has been executed.
      Excellent for progress-bar like functionality.

    * Has a ``map`` like function that uses tasks, called ``dmap``.

    * However, you rarely want to wait for these results in a web-environment.
      You'd rather want to use Ajax to poll the task status, which is
      available from a URL like ``celery/<task_id>/status/``. This view
      returns a JSON-serialized data structure containing the task status,
      and the return value if completed, or exception on failure.
      
API Reference Documentation
===========================

The `API Reference Documentation`_ is hosted at Github
(http://ask.github.com/celery)

.. _`API Reference Docmentation`: http://ask.github.com/celery/

Installation
=============

You can install ``celery`` either via the Python Package Index (PyPI)
or from source.

To install using ``pip``,::

    $ pip install celery

To install using ``easy_install``,::

    $ easy_install celery

If you have downloaded a source tarball you can install it
by doing the following,::

    $ python setup.py build
    # python setup.py install # as root

Usage
=====

Installing RabbitMQ
-------------------


Configuring your Django project
-------------------------------

Running the celery worker daemon
--------------------------------

To test this we'll be running the worker daemon in the foreground, so we can
see what's going on without consulting the logfile::

    $ python manage.py celeryd

However, in production you'll probably want to run the worker in the
background as daemon instead::

    $ python manage.py celeryd --daemon

For help on command line arguments to the worker daemon, you can execute the
help command::

    $ python manage.py help celeryd

**Note**: If you're using ``SQLite`` as the Django database back-end,
``celeryd`` will only be able to process one task at a time, this is
because ``SQLite`` doesn't allow concurrent writes.

Defining and executing tasks
----------------------------

**Please note** All of these tasks has to be stored in a real module, they can't
be defined in the python shell or ipython/bpython. This is because the celery
worker server needs access to the task function to be able to run it.
So while it looks like we use the python shell to define the tasks in these
examples, you can't do it this way. Put them in your Django applications
``tasks`` module (the worker daemon will automatically load any ``tasks.py``
file for all of the applications listed in ``settings.INSTALLED_APPS``.
Execution tasks using ``delay`` and ``apply_async`` can be done from the
python shell, but keep in mind that since arguments are pickled, you can't
use custom classes defined in the shell session.

While you can use regular functions, the recommended way is creating
a task class, this way you can cleanly upgrade the task to use the more
advanced features of celery later.

This is a task that basically does nothing but take some arguments,
and return value:

    >>> class MyTask(Task):
    ...     name = "myapp.mytask"
    ...     def run(self, some_arg, **kwargs):
    ...         logger = self.get_logger(**kwargs)
    ...         logger.info("Did something: %s" % some_arg)
    ...         return 42
    >>> tasks.register(MyTask)

Now if we want to execute this task, we can use the ``delay`` method of the
task class (this is a handy shortcut to the ``apply_async`` method which gives
you greater control of the task execution).

    >>> from myapp.tasks import MyTask
    >>> MyTask.delay(some_arg="foo")

At this point, the task has been sent to the message broker. The message
broker will hold on to the task until a celery worker server has successfully
picked it up.

Now the task has been executed, but to know what happened with the task we
have to check the celery logfile to see its return value and output.
This is because we didn't keep the ``AsyncResult`` object returned by
``delay``.

The ``AsyncResult`` lets us find out the state of the task, wait for the task to
finish and get its return value (or exception if the task failed).

So, let's execute the task again, but this time we'll keep track of the task:

    >>> result = MyTask.delay("do_something", some_arg="foo bar baz")
    >>> result.ready() # returns True if the task has finished processing.
    False
    >>> result.result # task is not ready, so no return value yet.
    None
    >>> result.get()   # Waits until the task is done and return the retval.
    42
    >>> result.result
    42
    >>> result.success() # returns True if the task didn't end in failure.
    True


If the task raises an exception, the ``result.success()`` will be ``False``,
and ``result.result`` will contain the exception instance raised.

Auto-discovery of tasks
-----------------------

``celery`` has an auto-discovery feature like the Django Admin, that
automatically loads any ``tasks.py`` module in the applications listed
in ``settings.INSTALLED_APPS``. This autodiscovery is used by the celery
worker to find registered tasks for your Django project.


Periodic Tasks
---------------

Periodic tasks are tasks that are run every ``n`` seconds. 
Here's an example of a periodic task:

    >>> from celery.task import tasks, PeriodicTask
    >>> from datetime import timedelta
    >>> class MyPeriodicTask(PeriodicTask):
    ...     name = "foo.my-periodic-task"
    ...     run_every = timedelta(seconds=30)
    ...
    ...     def run(self, **kwargs):
    ...         logger = self.get_logger(**kwargs)
    ...         logger.info("Running periodic task!")
    ...
    >>> tasks.register(MyPeriodicTask)

**Note:** Periodic tasks does not support arguments, as this doesn't
really make sense.

For periodic tasks to work you need to add ``celery`` to ``INSTALLED_APPS``,
and issue a ``syncdb``.

License
=======

This software is licensed under the ``New BSD License``. See the ``LICENSE``
file in the top distribution directory for the full license text.

.. # vim: syntax=rst expandtab tabstop=4 shiftwidth=4 shiftround

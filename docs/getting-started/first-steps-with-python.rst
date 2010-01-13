=========================
 First steps with Python
=========================

Creating a simple task
======================

We put tasks in a dedicated ``tasks.py`` module. Your tasks can be in
any module, but it's a good convention.

Our task is simple, just adding two numbers

``tasks.py``:

.. code-block:: python

    from celery.decorators import task

    @task
    def add(x, y):
        return x + y


Tasks in celery are actually classes inheriting from the ``Task`` class.
When you create a new task it is automatically registered in a registry, but
for this to happen in the worker you need to give a list of modules the worker
should import.

Configuration
=============

Celery needs a configuration module, usually called ``celeryconfig.py``.
This module must be importable and located in the Python path.

You can set a custom name for the configuration module with the
``CELERY_CONFIG_MODULE`` variable. In these examples we use the default name.

Let's create our ``celeryconfig.py``.

1. Start by making sure Python is able to import modules from the current
   directory::

        import os
        import sys
        sys.path.insert(0, os.getcwd())

2. Configure the broker::

        BROKER_HOST = "localhost"
        BROKER_PORT = 5672
        BROKER_USER = "myuser"
        BROKER_PASSWORD = "mypassword"
        BROKER_VHOST = "myvhost"

3. We don't want to store the results, so we'll just use the simplest
   backend available; the AMQP backend::

        CELERY_BACKEND = "amqp"

4. Finally, we list the modules to import. We only have a single module; the
   ``tasks.py`` module we added earlier::

        CELERY_IMPORTS = ("tasks", )

That's it.

There are more options available, like how many processes you want to process
work in parallel (the ``CELERY_CONCURRENCY`` setting), and we could use a
persistent result store backend, but for now, this should do. For all of
the options available, please see the :doc:`configuration directive
reference<../configuration>`.

Running the celery worker server
================================

To test this we'll be running the worker server in the foreground, so we can
see what's going on without consulting the logfile::

    $ celeryd --loglevel=INFO

However, in production you probably want to run the worker in the
background as a daemon. To do this you need to use to tools provided by your
platform, or something like `supervisord`_.

For example startup scripts see ``contrib/debian/init.d`` for using
``start-stop-daemon`` on Debian/Ubuntu, or ``contrib/mac/org.celeryq.*`` for using
``launchd`` on Mac OS X.

.. _`supervisord`: http://supervisord.org/

For a complete listing of the command line arguments available, with a short
description, you can use the help command::

    $  celeryd --help


Executing the task
==================

Now if we want to execute our task, we can use the
``delay`` method of the task class.
This is a handy shortcut to the ``apply_async`` method which gives
greater control of the task execution.
See :doc:`Executing Tasks<../userguide/executing>` for more information.

    >>> from tasks import add
    >>> add.delay(4, 4)
    <AsyncResult: 889143a6-39a2-4e52-837b-d80d33efb22d>

At this point, the task has been sent to the message broker. The message
broker will hold on to the task until a celery worker server has successfully
picked it up.

*Note* If everything is just hanging when you execute ``delay``, please check
that RabbitMQ is running, and that the user/password has access to the virtual
host you configured earlier.

Right now we have to check the celery worker logfiles to know what happened
with the task. This is because we didn't keep the ``AsyncResult`` object
returned by ``delay``.

The ``AsyncResult`` lets us find the state of the task, wait for the task to
finish and get its return value (or exception if the task failed).

So, let's execute the task again, but this time we'll keep track of the task:

    >>> result = add.delay(4, 4)
    >>> result.ready() # returns True if the task has finished processing.
    False
    >>> result.result # task is not ready, so no return value yet.
    None
    >>> result.get()   # Waits until the task is done and returns the retval.
    8
    >>> result.result # direct access to result, doesn't re-raise errors.
    8
    >>> result.successful() # returns True if the task didn't end in failure.
    True

If the task raises an exception, the return value of ``result.successful()``
will be ``False``, and ``result.result`` will contain the exception instance
raised by the task.

That's all for now! After this you should probably read the :doc:`User
Guide<../userguide/index>`.

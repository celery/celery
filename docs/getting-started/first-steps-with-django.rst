=========================
 First steps with Django
=========================

Configuring your Django project to use Celery
=============================================

You only need three simple steps to use celery with your Django project.

    1. Add ``celery`` to ``INSTALLED_APPS``.

    2. Create the celery database tables::

            $ python manage.py syncdb

    3. Configure celery to use the AMQP user and virtual host we created
        before, by adding the following to your ``settings.py``::

            BROKER_HOST = "localhost"
            BROKER_PORT = 5672
            BROKER_USER = "myuser"
            BROKER_PASSWORD = "mypassword"
            BROKER_VHOST = "myvhost"


That's it.

There are more options available, like how many processes you want to
work in parallel (the ``CELERY_CONCURRENCY`` setting). You can also
configure the backend used for storing task statuses. For now though, 
this should do. For all of the options available, please see the 
:doc:`configuration directive

reference<../configuration>`.

**Note:** If you're using SQLite as the Django database back-end,
``celeryd`` will only be able to process one task at a time, this is
because SQLite doesn't allow concurrent writes.



Running the celery worker server
================================

To test this we'll be running the worker server in the foreground, so we can
see what's going on without consulting the logfile::

    $ python manage.py celeryd

However, in production you probably want to run the worker in the
background as a daemon. To do this you need to use to tools provided by your
platform. See :doc:`daemon mode reference<../cookbook>`.

For a complete listing of the command line options available, use the help command::

    $ python manage.py help celeryd


Defining and executing tasks
============================

**Please note:** All the tasks have to be stored in a real module, they can't
be defined in the python shell or ipython/bpython. This is because the celery
worker server needs access to the task function to be able to run it.
Put them in the ``tasks`` module of your Django application. The
worker server  will automatically load any ``tasks.py`` file for all
of the applications listed in ``settings.INSTALLED_APPS``.
Executing tasks using ``delay`` and ``apply_async`` can be done from the
python shell, but keep in mind that since arguments are pickled, you can't
use custom classes defined in the shell session.

This is a task that adds two numbers:

.. code-block:: python

    from celery.decorators import task

    @task()
    def add(x, y):
        return x + y

To execute this task, we can use the ``delay`` method of the task class.
This is a handy shortcut to the ``apply_async`` method which gives
greater control of the task execution.
See :doc:`Executing Tasks<../userguide/executing>` for more information.

    >>> from myapp.tasks import MyTask
    >>> MyTask.delay(some_arg="foo")

At this point, the task has been sent to the message broker. The message
broker will hold on to the task until a celery worker server has successfully
picked it up.

*Note:* If everything is just hanging when you execute ``delay``, please check
that RabbitMQ is running, and that the user/password has access to the virtual
host you configured earlier.

Right now we have to check the celery worker log files to know what happened
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

=========================
 First steps with Django
=========================

Configuring your Django project to use Celery
=============================================

You need four simple steps to use celery with your Django project.

    1. Install the ``django-celery`` library::

        $ pip install django-celery

    2. Add ``djcelery`` to ``INSTALLED_APPS``.

    3. Add the following lines to ``settings.py``::

        import djcelery
        djcelery.setup_loader()

    4. Create the celery database tables::

        If you are using south_ for schema migrations, you'll want to::

            $ python manage.py migrate djcelery

        For those who are not using south, a normal ``syncdb`` will work::

            $ python manage.py syncdb

.. _south: http://pypi.python.org/pypi/South/

By default Celery uses `RabbitMQ`_ as the broker, but there are several
alternatives to choose from, see :ref:`celerytut-broker`.

All settings mentioned in the Celery documentation should be added
to your Django project's ``settings.py`` module. For example
we can configure the :setting:`BROKER_URL` setting to specify
what broker to use::

    BROKER_URL = "amqp://guest:guest@localhost:5672/"

That's it.

Special note for mod_wsgi users
-------------------------------

If you're using ``mod_wsgi`` to deploy your Django application you need to
include the following in your ``.wsgi`` module::

    import djcelery
    djcelery.setup_loader()

Defining and executing tasks
============================

Tasks are defined by wrapping functions in the ``@task`` decorator.
It is a common practice to put these in their own module named ``tasks.py``,
and the worker will automatically go through the apps in ``INSTALLED_APPS``
to import these modules.

For a simple demonstration we can create a new Django app called
``celerytest``.  To create this app you need to be in the directoryw
of your Django project where ``manage.py`` is located and execute::

    $ python manage.py startapp celerytest

After our new app has been created we can define our task by editing
a new file called ``celerytest/tasks.py``:

.. code-block:: python

    from celery.task import task

    @task()
    def add(x, y):
        return x + y

Our example task is pretty pointless, it just returns the sum of two
arguments, but it will do for demonstration, and it is referenced in many
parts of the Celery documentation.

Starting the worker process
===========================

You can start a worker instance by using the ``celeryd`` manage command::

    $ python manage.py celeryd --loglevel=info

In production you probably want to run the worker in the
background as a daemon, see `Running Celery as a daemon`_.
For a complete listing of the command line options available, use the help command::

    $ python manage.py help celeryd

.. _`Running Celery as a Daemon`:
    http://docs.celeryq.org/en/latest/cookbook/daemonizing.html

Executing our task
==================

Now that the worker is running we can open up a new terminal to actually
execute our task::

    >>> from celerytest.tasks import add

    >>> add.delay(2, 2)


The ``delay`` method is a handy shortcut to the ``apply_async`` method which
enables you to have greater control of the task execution.
To read more about executing tasks, including specifying the time at which
the task should execute see :ref:`guide-executing`.

.. note::

    Tasks need to be stored in a real module, they can't
    be defined in the python shell or ipython/bpython. This is because the
    worker server must be able to import the task function so that it can
    execute it.

The task should now be executed by the worker you started earlier,
and you can verify that by looking at the workers console output.

Applying a task returns an :class:`~celery.result.AsyncResult` instance,
which can be used to check the state of the task, wait for the task to finish
or get its return value (or if the task failed, the exception and traceback).

By default django-celery stores this state in the Django database,
you may consider choosing an alternate result backend or disabling
states alltogether (see :ref:`task-result-backends`).

To demonstrate how the results work we can execute the task again,
but this time keep the result instance returned::

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

Where to go from here
=====================

To learn more you should read the `Celery User Guide`_, and the
`Celery Documentation`_ in general


.. _`Celery User Guide`: http://docs.celeryproject.org/en/latest/userguide/
.. _`Celery Documentation`: http://docs.celeryproject.org/

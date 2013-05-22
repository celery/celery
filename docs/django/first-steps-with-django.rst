=========================
 First steps with Django
=========================

Configuring your Django project to use Celery
=============================================

You need four simple steps to use celery with your Django project.

    1. Install the ``django-celery`` library:

        .. code-block:: bash

            $ pip install django-celery

    2. Add the following lines to ``settings.py``:

        .. code-block:: python

            import djcelery
            djcelery.setup_loader()

    3. Add ``djcelery`` to ``INSTALLED_APPS``.

    4. Create the celery database tables.

        If you are using south_ for schema migrations, you'll want to:

        .. code-block:: bash

            $ python manage.py migrate djcelery

        For those who are not using south, a normal ``syncdb`` will work:

        .. code-block:: bash

            $ python manage.py syncdb

.. _south: http://pypi.python.org/pypi/South/

By default Celery uses `RabbitMQ`_ as the broker, but there are several
alternatives to choose from, see :ref:`celerytut-broker`.

In place of a ``celeryconfig.py`` configuration file, django-celery's settings should be added
to your Django project's ``settings.py`` module. 

Initially, configure the :setting:`BROKER_URL` setting to specify
what broker to use, and include :setting:`CELERY_IMPORTS` to identify where your tasks are located for import::

    BROKER_URL = 'amqp://guest:guest@localhost:5672/'
    CELERY_IMPORTS = ('myproject.app.module_containing_tasks',)

That's all you need for initial setup.

.. _`RabbitMQ`: http://www.rabbitmq.com/

Special note for mod_wsgi users
-------------------------------

If you're using ``mod_wsgi`` to deploy your Django application you need to
include the following in your ``.wsgi`` module::

    import djcelery
    djcelery.setup_loader()

Defining and calling tasks
==========================

Tasks are defined by wrapping functions in the ``@task`` decorator.
It is a common practice to put these in their own module named ``tasks.py``,
and the worker will automatically go through the apps in ``INSTALLED_APPS``
to import these modules.

For a simple demonstration create a new Django app called
``celerytest``.  To create this app you need to be in the directory
of your Django project where ``manage.py`` is located and execute:

.. code-block:: bash

    $ python manage.py startapp celerytest

Next you have to add the new app to ``INSTALLED_APPS`` so that your
Django project recognizes it.  This setting is a tuple/list so just
append ``celerytest`` as a new element at the end

.. code-block:: python

    INSTALLED_APPS = (
        ...,
        'djcelery',
        'celerytest',
    )

After the new app has been created and added to ``INSTALLED_APPS``,
you can define your tasks by creating a new file called ``celerytest/tasks.py``:

.. code-block:: python

    from celery import task

    @task()
    def add(x, y):
        return x + y

Our example task is pretty pointless, it just returns the sum of two
arguments, but it will do for demonstration, and it is referred to in many
parts of the Celery documentation.

.. admonition:: Relative Imports

    You have to be consistent in how you import the task module, e.g. if
    you have ``project.app`` in ``INSTALLED_APPS`` then you also
    need to import the tasks ``from project.app`` or else the names
    of the tasks will be different.

    See :ref:`task-naming-relative-imports`

Starting the worker process
===========================

In a production environment you will want to run the worker in the background
as a daemon - see :ref:`daemonizing` - but for testing and
development it is useful to be able to start a worker instance by using the
``celery worker`` manage command, much as you would use Django's runserver:

.. code-block:: bash

    $ python manage.py celery worker --loglevel=info

For a complete listing of the command-line options available,
use the help command:

.. code-block:: bash

    $ python manage.py celery help

Calling our task
================

Now that the worker is running, open up a new python interactive shell
with ``python manage.py shell`` to actually call the task you defined::

    >>> from celerytest.tasks import add

    >>> add.delay(2, 2)


Note that if you open a regular python shell by simply running ``python``
you will need to import your Django application's settings by running::

    # Replace 'myproject' with your project's name
    >>> from myproject import settings


The ``delay`` method used above is a handy shortcut to the ``apply_async`` 
method which enables you to have greater control of the task execution.
To read more about calling tasks, including specifying the time at which
the task should be processed see :ref:`guide-calling`.

.. note::

    Tasks need to be stored in a real module, they can't
    be defined in the python shell or IPython/bpython. This is because the
    worker server must be able to import the task function.

The task should now be processed by the worker you started earlier,
and you can verify that by looking at the worker's console output.

Calling a task returns an :class:`~celery.result.AsyncResult` instance,
which can be used to check the state of the task, wait for the task to finish
or get its return value (or if the task failed, the exception and traceback).

By default django-celery stores this state in the Django database.
You may consider choosing an alternate result backend or disabling
states alltogether (see :ref:`task-result-backends`).

To demonstrate how the results work call the task again, but this time
keep the result instance returned::

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

Celery init.d
-----------------

From here you'll want to easily be able to start, stop and restart Celery processes in the background. 
Celery provides a very useful default `extra/generic-init.d/`_ script to enable this.

.. _`extra/generic-init.d/`:
    http://github.com/celery/celery/tree/3.0/extra/generic-init.d/


Copy that file as is and place it in :file:`/etc/init.d/celeryd`.

Celery config
--------------

Next, you want to create the :file:`/etc/default/celeryd` that contains your default Celery settings that are picked up by the :file:`init.d` script.

See :ref:`generic-initd-celeryd-django-example` for a django-celery sample.


What's Next?
=============

Review :ref:`daemonizing`, and make sure that you're running celery as an unprivileged user.

To learn more you should read the `Celery User Guide`_, and the
`Celery Documentation`_ in general.


.. _`Celery User Guide`: http://docs.celeryproject.org/en/latest/userguide/
.. _`Celery Documentation`: http://docs.celeryproject.org/

.. _django-first-steps:

=========================
 First steps with Django
=========================

Using Celery with Django
========================

.. note::

    Previous versions of Celery required a separate library to work with Django,
    but since 3.1 this is no longer the case. Django is supported out of the
    box now so this document only contains a basic way to integrate Celery and
    Django.  You will use the same API as non-Django users so it's recommended that
    you read the :ref:`first-steps` tutorial
    first and come back to this tutorial.  When you have a working example you can
    continue to the :ref:`next-steps` guide.

To use Celery with your Django project you must first define
an instance of the Celery library (called an "app")

If you have a modern Django project layout like::

    - proj/
      - proj/__init__.py
      - proj/settings.py
      - proj/urls.py
    - manage.py

then the recommended way is to create a new `proj/proj/celery.py` module
that defines the Celery instance:

:file: `proj/proj/celery.py`

.. literalinclude:: ../../examples/django/proj/celery.py

Then you need to import this app in your :file:`proj/proj/__init__.py`
module.  This ensures that the app is loaded when Django starts
so that the ``@shared_task`` decorator (mentioned later) will use it:

:file:`proj/proj/__init__.py`:

.. literalinclude:: ../../examples/django/proj/__init__.py

Note that this example project layout is suitable for larger projects,
for simple projects you may use a single contained module that defines
both the app and tasks, like in the :ref:`tut-celery` tutorial.

Let's break down what happens in the first module,
first we import absolute imports from the future, so that our
``celery.py`` module will not clash with the library:

.. code-block:: python

    from __future__ import absolute_import

Then we set the default :envvar:`DJANGO_SETTINGS_MODULE` environment variable
for the :program:`celery` command-line program:

.. code-block:: python

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')

You don't need this line, but it saves you from always passing in the
settings module to the celery program.  It must always come before
creating the app instances, which is what we do next:

.. code-block:: python

    app = Celery('proj')

This is our instance of the library, you can have many instances
but there's probably no reason for that when using Django.

We also add the Django settings module as a configuration source
for Celery.  This means that you don't have to use multiple
configuration files, and instead configure Celery directly
from the Django settings; but you can also separate them if wanted.

The uppercase name-space means that all Celery configuration options
must be specified in uppercase instead of lowercase, and start with
``CELERY_``, so e.g. the :setting:`task_always_eager`` setting
becomes ``CELERY_TASK_ALWAYS_EAGER``, and the :setting:`broker_url`
setting becomes ``CELERY_BROKER_URL``.

You can pass the object directly here, but using a string is better since
then the worker doesn't have to serialize the object.

.. code-block:: python

    app.config_from_object('django.conf:settings', namespace='CELERY')

Next, a common practice for reusable apps is to define all tasks
in a separate ``tasks.py`` module, and Celery does have a way to
auto-discover these modules:

.. code-block:: python

    app.autodiscover_tasks()

With the line above Celery will automatically discover tasks from all
of your installed apps, following the ``tasks.py`` convention::

    - app1/
        - tasks.py
        - models.py
    - app2/
        - tasks.py
        - models.py


This way you do not have to manually add the individual modules
to the :setting:`CELERY_IMPORTS <imports>` setting.  The ``lambda`` so that the
auto-discovery can happen only when needed, and so that importing your
module will not evaluate the Django settings object.

Finally, the ``debug_task`` example is a task that dumps
its own request information.  This is using the new ``bind=True`` task option
introduced in Celery 3.1 to easily refer to the current task instance.

Using the ``@shared_task`` decorator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The tasks you write will probably live in reusable apps, and reusable
apps cannot depend on the project itself, so you also cannot import your app
instance directly.

The ``@shared_task`` decorator lets you create tasks without having any
concrete app instance:

:file:`demoapp/tasks.py`:

.. literalinclude:: ../../examples/django/demoapp/tasks.py


.. seealso::

    You can find the full source code for the Django example project at:
    https://github.com/celery/celery/tree/3.1/examples/django/

Using the Django ORM/Cache as a result backend.
-----------------------------------------------

The [``django-celery``](https://github.com/celery/django-celery) library defines
result backends that uses the Django ORM and Django Cache frameworks.

To use this with your project you need to follow these four steps:

1. Install the :pypi:`django-celery` library:

    .. code-block:: console

        $ pip install django-celery

2. Add ``djcelery`` to ``INSTALLED_APPS``.

3. Create the celery database tables.

    This step will create the tables used to store results
    when using the database result backend and the tables used
    by the database periodic task scheduler.  You can skip
    this step if you don't use these.

    If you are using Django 1.7+ or south_, you'll want to:

    .. code-block:: console

        $ python manage.py migrate djcelery

    For those who are on Django 1.6 or lower and not using south, a normal
    ``syncdb`` will work:

    .. code-block:: console

        $ python manage.py syncdb

4.  Configure celery to use the :pypi:`django-celery` backend.

    For the database backend you must use:

    .. code-block:: python

        app.conf.update(
            result_backend='djcelery.backends.database:DatabaseBackend',
        )

    For the cache backend you can use:

    .. code-block:: python

        app.conf.update(
            result_backend='djcelery.backends.cache:CacheBackend',
        )

    If you have connected Celery to your Django settings then you can
    add this directly into your settings module (without the
    ``app.conf.update`` part)



.. _south: http://pypi.python.org/pypi/South/

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
:program:`celery worker` manage command, much as you would use Django's
:command:`manage.py runserver`:

.. code-block:: console

    $ celery -A proj worker -l info

For a complete listing of the command-line options available,
use the help command:

.. code-block:: console

    $ celery help

Where to go from here
=====================

If you want to learn more you should continue to the
:ref:`Next Steps <next-steps>` tutorial, and after that you
can study the :ref:`User Guide <guide>`.

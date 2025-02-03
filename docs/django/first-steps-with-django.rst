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
    Django. You'll use the same API as non-Django users so you're recommended
    to read the :ref:`first-steps` tutorial
    first and come back to this tutorial. When you have a working example you can
    continue to the :ref:`next-steps` guide.

.. note::

    Celery 5.5.x supports Django 2.2 LTS or newer versions.
    Please use Celery 5.2.x for versions older than Django 2.2 or Celery 4.4.x if your Django version is older than 1.11.

To use Celery with your Django project you must first define
an instance of the Celery library (called an "app")

If you have a modern Django project layout like::

    - proj/
      - manage.py
      - proj/
        - __init__.py
        - settings.py
        - urls.py

then the recommended way is to create a new `proj/proj/celery.py` module
that defines the Celery instance:

:file: `proj/proj/celery.py`

.. literalinclude:: ../../examples/django/proj/celery.py

Then you need to import this app in your :file:`proj/proj/__init__.py`
module. This ensures that the app is loaded when Django starts
so that the ``@shared_task`` decorator (mentioned later) will use it:

:file:`proj/proj/__init__.py`:

.. literalinclude:: ../../examples/django/proj/__init__.py

Note that this example project layout is suitable for larger projects,
for simple projects you may use a single contained module that defines
both the app and tasks, like in the :ref:`tut-celery` tutorial.

Let's break down what happens in the first module,
first, we set the default :envvar:`DJANGO_SETTINGS_MODULE` environment
variable for the :program:`celery` command-line program:

.. code-block:: python

    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'proj.settings')

You don't need this line, but it saves you from always passing in the
settings module to the ``celery`` program. It must always come before
creating the app instances, as is what we do next:

.. code-block:: python

    app = Celery('proj')

This is our instance of the library, you can have many instances
but there's probably no reason for that when using Django.

We also add the Django settings module as a configuration source
for Celery. This means that you don't have to use multiple
configuration files, and instead configure Celery directly
from the Django settings; but you can also separate them if wanted.

.. code-block:: python

    app.config_from_object('django.conf:settings', namespace='CELERY')

The uppercase name-space means that all
:ref:`Celery configuration options <configuration>`
must be specified in uppercase instead of lowercase, and start with
``CELERY_``, so for example the :setting:`task_always_eager` setting
becomes ``CELERY_TASK_ALWAYS_EAGER``, and the :setting:`broker_url`
setting becomes ``CELERY_BROKER_URL``. This also applies to the
workers settings, for instance, the :setting:`worker_concurrency`
setting becomes ``CELERY_WORKER_CONCURRENCY``.

For example, a Django project's configuration file might include:

.. code-block:: python
    :caption: settings.py

    ...

    # Celery Configuration Options
    CELERY_TIMEZONE = "Australia/Tasmania"
    CELERY_TASK_TRACK_STARTED = True
    CELERY_TASK_TIME_LIMIT = 30 * 60

You can pass the settings object directly instead, but using a string
is better since then the worker doesn't have to serialize the object.
The ``CELERY_`` namespace is also optional, but recommended (to
prevent overlap with other Django settings).

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


This way you don't have to manually add the individual modules
to the :setting:`CELERY_IMPORTS <imports>` setting.

Finally, the ``debug_task`` example is a task that dumps
its own request information. This is using the new ``bind=True`` task option
introduced in Celery 3.1 to easily refer to the current task instance.

Using the ``@shared_task`` decorator
------------------------------------

The tasks you write will probably live in reusable apps, and reusable
apps cannot depend on the project itself, so you also cannot import your app
instance directly.

The ``@shared_task`` decorator lets you create tasks without having any
concrete app instance:

:file:`demoapp/tasks.py`:

.. literalinclude:: ../../examples/django/demoapp/tasks.py


.. seealso::

    You can find the full source code for the Django example project at:
    https://github.com/celery/celery/tree/main/examples/django/

Trigger tasks at the end of the database transaction
----------------------------------------------------

A common pitfall with Django is triggering a task immediately and not wait until
the end of the database transaction, which means that the Celery task may run
before all changes are persisted to the database. For example:

.. code-block:: python

    # views.py
    def create_user(request):
        # Note: simplified example, use a form to validate input
        user = User.objects.create(username=request.POST['username'])
        send_email.delay(user.pk)
        return HttpResponse('User created')

    # task.py
    @shared_task
    def send_email(user_pk):
        user = User.objects.get(pk=user_pk)
        # send email ...

In this case, the ``send_email`` task could start before the view has committed
the transaction to the database, and therefore the task may not be able to find
the user.

A common solution is to use Django's `on_commit`_ hook to trigger the task
after the transaction has been committed:

.. _on_commit: https://docs.djangoproject.com/en/stable/topics/db/transactions/#django.db.transaction.on_commit

.. code-block:: diff

    - send_email.delay(user.pk)
    + transaction.on_commit(lambda: send_email.delay(user.pk))

.. versionadded:: 5.4

Since this is such a common pattern, Celery 5.4 introduced a handy shortcut for this,
using a :class:`~celery.contrib.django.task.DjangoTask`. Instead of calling
:meth:`~celery.app.task.Task.delay`, you should call
:meth:`~celery.contrib.django.task.DjangoTask.delay_on_commit`:

.. code-block:: diff

    - send_email.delay(user.pk)
    + send_email.delay_on_commit(user.pk)


This API takes care of wrapping the call into the `on_commit`_ hook for you.
In rare cases where you want to trigger a task without waiting, the existing
:meth:`~celery.app.task.Task.delay` API is still available.

One key difference compared to the ``delay`` method, is that ``delay_on_commit``
will NOT return the task ID back to the caller. The task is not sent to the broker
when you call the method, only when the Django transaction finishes. If you need the
task ID, best to stick to :meth:`~celery.app.task.Task.delay`.

This task class should be used automatically if you've follow the setup steps above.
However, if your app :ref:`uses a custom task base class <task-custom-classes>`,
you'll need inherit from :class:`~celery.contrib.django.task.DjangoTask` instead of
:class:`~celery.app.task.Task` to get this behaviour.

Extensions
==========

.. _django-celery-results:

``django-celery-results`` - Using the Django ORM/Cache as a result backend
--------------------------------------------------------------------------

The :pypi:`django-celery-results` extension provides result backends
using either the Django ORM, or the Django Cache framework.

To use this with your project you need to follow these steps:

#. Install the :pypi:`django-celery-results` library:

    .. code-block:: console

        $ pip install django-celery-results

#. Add ``django_celery_results`` to ``INSTALLED_APPS`` in your
   Django project's :file:`settings.py`::

        INSTALLED_APPS = (
            ...,
            'django_celery_results',
        )

   Note that there is no dash in the module name, only underscores.

#. Create the Celery database tables by performing a database migrations:

    .. code-block:: console

        $ python manage.py migrate django_celery_results

#. Configure Celery to use the :pypi:`django-celery-results` backend.

    Assuming you are using Django's :file:`settings.py` to also configure
    Celery, add the following settings:

    .. code-block:: python

        CELERY_RESULT_BACKEND = 'django-db'

    When using the cache backend, you can specify a cache defined within
    Django's CACHES setting.

    .. code-block:: python

        CELERY_RESULT_BACKEND = 'django-cache'

        # pick which cache from the CACHES setting.
        CELERY_CACHE_BACKEND = 'default'

        # django setting.
        CACHES = {
            'default': {
                'BACKEND': 'django.core.cache.backends.db.DatabaseCache',
                'LOCATION': 'my_cache_table',
            }
        }

    For additional configuration options, view the
    :ref:`conf-result-backend` reference.


``django-celery-beat`` - Database-backed Periodic Tasks with Admin interface.
-----------------------------------------------------------------------------

See :ref:`beat-custom-schedulers` for more information.

Starting the worker process
===========================

In a production environment you'll want to run the worker in the background
as a daemon - see :ref:`daemonizing` - but for testing and
development it is useful to be able to start a worker instance by using the
:program:`celery worker` manage command, much as you'd use Django's
:command:`manage.py runserver`:

.. code-block:: console

    $ celery -A proj worker -l INFO

For a complete listing of the command-line options available,
use the help command:

.. code-block:: console

    $ celery --help

Where to go from here
=====================

If you want to learn more you should continue to the
:ref:`Next Steps <next-steps>` tutorial, and after that you
can study the :ref:`User Guide <guide>`.

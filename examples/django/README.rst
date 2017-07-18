==============================================================
 Example Django project using Celery
==============================================================

Contents
========

``proj/``
---------

This is a project in itself, created using
``django-admin.py startproject proj``, and then the settings module
(``proj/settings.py``) was modified to add ``demoapp`` to
``INSTALLED_APPS``

``proj/celery.py``
----------

This module contains the Celery application instance for this project,
we take configuration from Django settings and use ``autodiscover_tasks`` to
find task modules inside all packages listed in ``INSTALLED_APPS``.

``demoapp/``
------------

Example generic app.  This is decoupled from the rest of the project by using
the ``@shared_task`` decorator.  This decorator returns a proxy that always
points to the currently active Celery instance.

Installing requirements
=======================

The settings file assumes that ``rabbitmq-server`` is running on ``localhost``
using the default ports. More information here:

http://docs.celeryproject.org/en/latest/getting-started/brokers/rabbitmq.html

In addition, some Python requirements must also be satisfied:

.. code-block:: console

    $ pip install -r requirements.txt

Starting the worker
===================

.. code-block:: console

    $ celery -A proj worker -l info

Running a task
===================

.. code-block:: console

    $ python ./manage.py shell
    >>> from demoapp.tasks import add, mul, xsum
    >>> res = add.delay(2,3)
    >>> res.get()
    5

==============================================================
 Example Django project using Celery
==============================================================

Contents
========

:file:`proj/`
-------------

This is the project iself, created using
:program:`django-admin.py startproject proj`, and then the settings module
(:file:`proj/settings.py`) was modified to add ``tasks`` and ``demoapp`` to
``INSTALLED_APPS``

:file:`tasks/`
--------------

This app contains the Celery application instance for this project,
we take configuration from Django settings and use ``autodiscover_tasks`` to
find task modules inside all packages listed in ``INSTALLED_APPS``.

:file:`demoapp/`
----------------

Example generic app.  This is decoupled from the rest of the project by using
the ``@shared_task`` decorator.  Shared tasks are shared between all Celery
instances.


Starting the worker
===================

.. code-block:: bash

    $ DJANGO_SETTINGS_MODULE='proj.settings' celery -A tasks worker -l info

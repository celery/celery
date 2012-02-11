.. _broker-django:

===========================
 Using the Django Database
===========================

.. _broker-django-installation:

Installation
============

.. _broker-django-configuration:

Configuration
=============

The database transport uses the Django `DATABASE_*` settings for database
configuration values.

#. Set your broker transport::

    BROKER_URL = "django://"

#. Add :mod:`django.transport.kombu` to `INSTALLED_APPS`::

    INSTALLED_APPS = ("django.transport.kombu", )

#. Verify your database settings::

    DATABASE_ENGINE = "mysql"
    DATABASE_NAME = "mydb"
    DATABASE_USER = "myuser"
    DATABASE_PASSWORD = "secret"

  The above is just an example, if you haven't configured your database before
  you should read the Django database settings reference:
  http://docs.djangoproject.com/en/1.1/ref/settings/#database-engine

#. Sync your database schema::

    $ python manage.py syncdb

.. _broker-django-limitations:

Limitations
===========

The Django database transport does not currently support:

    * Remote control commands (celeryev, broadcast)
    * Events, including the Django Admin monitor.
    * Using more than a few workers (can lead to messages being executed
      multiple times).

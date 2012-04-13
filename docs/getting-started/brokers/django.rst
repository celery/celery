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

#. Add :mod:`kombu.transport.django` to `INSTALLED_APPS`::

    INSTALLED_APPS = ("kombu.transport.django", )

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

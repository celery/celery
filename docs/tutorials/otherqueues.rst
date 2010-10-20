.. _tut-otherqueues:

==========================================================
 Using Celery with Redis/Database as the messaging queue.
==========================================================

.. contents::
    :local:

.. _otherqueues-installation:

Installation
============

.. _otherqueues-redis:

Redis
=====

For the Redis support you have to install the Python redis client::

    $ pip install -U redis

.. _otherqueues-redis-conf:

Configuration
-------------

Configuration is easy, set the transport, and configure the location of
your Redis database::

    BROKER_BACKEND = "redis"

    BROKER_HOST = "localhost"  # Maps to redis host.
    BROKER_PORT = 6379         # Maps to redis port.
    BROKER_VHOST = "0"         # Maps to database number.

.. _otherqueues-database:

Database
========

.. _otherqueues-database-conf:

Configuration
-------------

The database backend uses the Django ``DATABASE_*`` settings for database
configuration values.

#. Set your carrot backend::

    CARROT_BACKEND = "ghettoq.taproot.Database"


#. Add :mod:`ghettoq` to ``INSTALLED_APPS``::

    INSTALLED_APPS = ("ghettoq", )


#. Verify you database settings::

    DATABASE_ENGINE = "mysql"
    DATABASE_NAME = "mydb"
    DATABASE_USER = "myuser"
    DATABASE_PASSWORD = "secret"

  The above is just an example, if you haven't configured your database before
  you should read the Django database settings reference:
  http://docs.djangoproject.com/en/1.1/ref/settings/#database-engine


#. Sync your database schema.

    When using Django::

        $ python manage.py syncdb

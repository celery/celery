==========================================================
 Using Celery with Redis/Database as the messaging queue.
==========================================================

There's a plug-in for celery that enables the use of Redis or an SQL database
as the messaging queue. This is not part of celery itself, but exists as
an extension to `carrot`_.

.. _`carrot`: http://ask.github.com/carrot
.. _`ghettoq`: http://ask.github.com/ghettoq

Installation
============

You need to install the `ghettoq`_ library::

    $ pip install -U ghettoq

Redis
=====

For the Redis support you have to install the Python redis client::

    $ pip install -U redis

Configuration
-------------

Configuration is easy, set the carrot backend, and configure the location of
your Redis database::

    CARROT_BACKEND = "ghettoq.taproot.Redis"

    BROKER_HOST = "localhost"  # Maps to redis host.
    BROKER_PORT = 6379         # Maps to redis port.
    BROKER_VHOST = "celery"    # Maps to database name.

Database
========

Configuration
-------------

The database backend uses the Django ``DATABASE_*`` settings for database
configuration values.

#. Set your carrot backend::

    CARROT_BACKEND = "ghettoq.taproot.Database"


#. Add ``ghettoq`` to ``INSTALLED_APPS``::

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

  Or if you're not using django, but the default loader instead run
  ``celeryinit``::

        $ celeryinit

Important notes
---------------

These message queues does not have the concept of exchanges and routing keys,
there's only the queue entity. As a result of this you need to set the
name of the exchange to be the same as the queue::

    CELERY_DEFAULT_EXCHANGE = "tasks"

or in a custom queue-mapping:

.. code-block:: python

    CELERY_QUEUES = {
        "tasks": {"exchange": "tasks"},
        "feeds": {"exchange": "feeds"},
    }

This isn't a problem if you use the default queue setting, as the default is
already using the same name for queue/exchange.

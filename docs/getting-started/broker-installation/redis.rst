.. _broker-redis:

=============
 Using Redis
=============

.. _broker-redis-installation:

Installation
============

For the Redis support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
ehter the `celery-with-redis`_, or the `django-celery-with-redis` bundles::

    $ pip install -U celery-with-redis

.. _`celery-with-redis`:
    http://pypi.python.org/pypi/celery-with-redis
.. _`django-celery-with-redis`:
    http://pypi.python.org/pypi/django-celery-with-redis

.. _broker-redis-configuration:

Configuration
=============

Configuration is easy, set the transport, and configure the location of
your Redis database::

    BROKER_URL = "redis://localhost:6379/0"


Where the URL is in the format of::

    redis://userid:password@hostname:port/db_number

.. _redis-results-configuration:

Results
-------

You probably also want to store results in Redis::

    CELERY_RESULT_BACKEND = "redis"
    CELERY_REDIS_HOST = "localhost"
    CELERY_REDIS_PORT = 6379
    CELERY_REDIS_DB = 0

For a complete list of options supported by the Redis result backend see
:ref:`conf-redis-result-backend`

If you don't intend to consume results you should disable them::

    CELERY_IGNORE_RESULT = True

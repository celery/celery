.. _broker-redis:

=============
 Using Redis
=============

.. _broker-redis-installation:

Installation
============

For the Redis support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
either the `celery-with-redis`_, or the `django-celery-with-redis` bundles:

.. code-block:: bash

    $ pip install -U celery-with-redis

.. _`celery-with-redis`:
    http://pypi.python.org/pypi/celery-with-redis
.. _`django-celery-with-redis`:
    http://pypi.python.org/pypi/django-celery-with-redis

.. _broker-redis-configuration:

Configuration
=============

Configuration is easy, just configure the location of
your Redis database::

    BROKER_URL = 'redis://localhost:6379/0'

Where the URL is in the format of::

    redis://:password@hostname:port/db_number

all fields after the scheme are optional, and will default to localhost on port 6379,
using database 0.

.. _redis-visibility_timeout:

Visibility Timeout
------------------

The visibility timeout defines the number of seconds to wait
for the worker to acknowledge the task before the message is redelivered
to another worker.  Be sure to see :ref:`redis-caveats` below.

This option is set via the :setting:`BROKER_TRANSPORT_OPTIONS` setting::

    BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 3600}  # 1 hour.

The default visibility timeout for Redis is 1 hour.

.. _redis-results-configuration:

Results
-------

If you also want to store the state and return values of tasks in Redis,
you should configure these settings::

    CELERY_RESULT_BACKEND = 'redis://localhost:6379/0'

For a complete list of options supported by the Redis result backend, see
:ref:`conf-redis-result-backend`

.. _redis-caveats:

Caveats
=======

- If a task is not acknowledged within the :ref:`redis-visibility_timeout`
  the task will be redelivered to another worker and executed.

    This causes problems with ETA/countdown/retry tasks where the
    time to execute exceeds the visibility timeout; in fact if that
    happens it will be executed again, and again in a loop.

    So you have to increase the visibility timeout to match
    the time of the longest ETA you are planning to use.

    Note that Celery will redeliver messages at worker shutdown,
    so having a long visibility timeout will only delay the redelivery
    of 'lost' tasks in the event of a power failure or forcefully terminated
    workers.

    Periodic tasks will not be affected by the visibility timeout,
    as this is a concept separate from ETA/countdown.

    You can increase this timeout by configuring a transport option
    with the same name:

        BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 43200}

    The value must be an int describing the number of seconds.


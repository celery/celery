.. _broker-redis:

=============
 Using Redis
=============

.. _broker-redis-installation:

Installation
============

For the Redis support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[redis]`` :ref:`bundle <bundles>`:

.. code-block:: bash

    $ pip install -U celery[redis]

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

- Broadcast messages will be seen by all virtual hosts by default.

    You have to set a transport option to prefix the messages so that
    they will only be received by the active virtual host::

        BROKER_TRANSPORT_OPTIONS = {'fanout_prefix': True}

    Note that you will not be able to communicate with workers running older
    versions or workers that does not have this setting enabled.

    This setting will be the default in the future, so better to migrate
    sooner rather than later.

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


- Monitoring events (as used by flower and other tools) are global
  and is not affected by the virtual host setting.

    This is caused by a limitation in Redis.  The Redis PUB/SUB channels
    are global and not affected by the database number.

- Redis may evict keys from the database in some situations

    If you experience an error like::

        InconsistencyError, Probably the key ('_kombu.binding.celery') has been
        removed from the Redis database.

    you may want to configure the redis-server to not evict keys by setting
    the ``timeout`` parameter to 0.

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

.. code-block:: console

    $ pip install -U celery[redis]

.. _broker-redis-configuration:

Configuration
=============

Configuration is easy, just configure the location of
your Redis database:

.. code-block:: python

    app.conf.broker_url = 'redis://localhost:6379/0'

Where the URL is in the format of:

.. code-block:: text

    redis://:password@hostname:port/db_number

all fields after the scheme are optional, and will default to ``localhost``
on port 6379, using database 0.

If a Unix socket connection should be used, the URL needs to be in the format:

.. code-block:: text

    redis+socket:///path/to/redis.sock

Specifying a different database number when using a Unix socket is possible
by adding the ``virtual_host`` parameter to the URL:

.. code-block:: text

    redis+socket:///path/to/redis.sock?virtual_host=db_number

.. _redis-visibility_timeout:

Visibility Timeout
------------------

The visibility timeout defines the number of seconds to wait
for the worker to acknowledge the task before the message is redelivered
to another worker. Be sure to see :ref:`redis-caveats` below.

This option is set via the :setting:`broker_transport_options` setting:

.. code-block:: python

    app.conf.broker_transport_options = {'visibility_timeout': 3600}  # 1 hour.

The default visibility timeout for Redis is 1 hour.

.. _redis-results-configuration:

Results
-------

If you also want to store the state and return values of tasks in Redis,
you should configure these settings::

    app.conf.result_backend = 'redis://localhost:6379/0'

For a complete list of options supported by the Redis result backend, see
:ref:`conf-redis-result-backend`

.. _redis-caveats:

Caveats
=======

.. _redis-caveat-fanout-prefix:

Fanout prefix
-------------

Broadcast messages will be seen by all virtual hosts by default.

You have to set a transport option to prefix the messages so that
they will only be received by the active virtual host:

.. code-block:: python

    app.conf.broker_transport_options = {'fanout_prefix': True}

Note that you won't be able to communicate with workers running older
versions or workers that doesn't have this setting enabled.

This setting will be the default in the future, so better to migrate
sooner rather than later.

.. _redis-caveat-fanout-patterns:

Fanout patterns
---------------

Workers will receive all task related events by default.

To avoid this you must set the ``fanout_patterns`` fanout option so that
the workers may only subscribe to worker related events:

.. code-block:: python

    app.conf.broker_transport_options = {'fanout_patterns': True}

Note that this change is backward incompatible so all workers in the
cluster must have this option enabled, or else they won't be able to
communicate.

This option will be enabled by default in the future.

Visibility timeout
------------------

If a task isn't acknowledged within the :ref:`redis-visibility_timeout`
the task will be redelivered to another worker and executed.

This causes problems with ETA/countdown/retry tasks where the
time to execute exceeds the visibility timeout; in fact if that
happens it will be executed again, and again in a loop.

So you have to increase the visibility timeout to match
the time of the longest ETA you're planning to use.

Note that Celery will redeliver messages at worker shutdown,
so having a long visibility timeout will only delay the redelivery
of 'lost' tasks in the event of a power failure or forcefully terminated
workers.

Periodic tasks won't be affected by the visibility timeout,
as this is a concept separate from ETA/countdown.

You can increase this timeout by configuring a transport option
with the same name:

.. code-block:: python

    app.conf.broker_transport_options = {'visibility_timeout': 43200}

The value must be an int describing the number of seconds.

Key eviction
------------

Redis may evict keys from the database in some situations

If you experience an error like:

.. code-block:: text

    InconsistencyError: Probably the key ('_kombu.binding.celery') has been
    removed from the Redis database.

then you may want to configure the :command:`redis-server` to not evict keys
by setting the ``timeout`` parameter to 0 in the redis configuration file.

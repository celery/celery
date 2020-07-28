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

    $ pip install -U "celery[redis]"

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

It is also easy to connect directly to a list of Redis Sentinel:

.. code-block:: python

    app.conf.broker_url = 'sentinel://localhost:26379;sentinel://localhost:26380;sentinel://localhost:26381'
    app.conf.broker_transport_options = { 'master_name': "cluster1" }

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
:ref:`conf-redis-result-backend`.

If you are using Sentinel, you should specify the master_name using the :setting:`result_backend_transport_options` setting:

.. code-block:: python

    app.conf.result_backend_transport_options = {'master_name': "mymaster"}


.. _redis-caveats:

Caveats
=======

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

Group result ordering
---------------------

Versions of Celery up to and including 4.4.6 used an unsorted list to store
result objects for groups in the Redis backend. This can cause those results to
be be returned in a different order to their associated tasks in the original
group instantiation.

Celery 4.4.7 and up introduce an opt-in behaviour which fixes this issue and
ensures that group results are returned in the same order the tasks were
defined, matching the behaviour of other backends. This change is incompatible
with workers running versions of Celery without this feature, so the feature
must be turned on using the boolean `result_chord_ordered` option of the
:setting:`result_backend_transport_options` setting, like so:

.. code-block:: python

    app.conf.result_backend_transport_options = {
        'result_chord_ordered': True
    }

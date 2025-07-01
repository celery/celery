.. _broker-valkey:

=============
 Using Valkey
=============

.. _broker-valkey-installation:

Installation
============

For the Valkey support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[valkey]`` :ref:`bundle <bundles>`:

.. code-block:: console

    $ pip install -U "celery[valkey]"

.. _broker-valkey-configuration:

Configuration
=============

Configuration is easy, just configure the location of
your Valkey database:

.. code-block:: python

    app.conf.broker_url = 'valkey://localhost:6379/0'

Where the URL is in the format of:

.. code-block:: text

    valkey://:password@hostname:port/db_number

all fields after the scheme are optional, and will default to ``localhost``
on port 6379, using database 0.

If a Unix socket connection should be used, the URL needs to be in the format:

.. code-block:: text

    valkey+socket:///path/to/valkey.sock

Specifying a different database number when using a Unix socket is possible
by adding the ``virtual_host`` parameter to the URL:

.. code-block:: text

    valkey+socket:///path/to/valkey.sock?virtual_host=db_number

It is also easy to connect directly to a list of Valkey Sentinel:

.. code-block:: python

    app.conf.broker_url = 'valkeysentinel://localhost:26379;sentinel://localhost:26380;sentinel://localhost:26381'
    app.conf.broker_transport_options = { 'master_name': "cluster1" }

Additional options can be passed to the Sentinel client using ``sentinel_kwargs``:

.. code-block:: python

    app.conf.broker_transport_options = { 'sentinel_kwargs': { 'password': "password" } }

.. _valkey-visibility_timeout:

Visibility Timeout
------------------

The visibility timeout defines the number of seconds to wait
for the worker to acknowledge the task before the message is redelivered
to another worker. Be sure to see :ref:`valkey-caveats` below.

This option is set via the :setting:`broker_transport_options` setting:

.. code-block:: python

    app.conf.broker_transport_options = {'visibility_timeout': 3600}  # 1 hour.

The default visibility timeout for Valkey is 1 hour.

.. _valkey-results-configuration:

Results
-------

If you also want to store the state and return values of tasks in Valkey,
you should configure these settings::

    app.conf.result_backend = 'valkey://localhost:6379/0'

For a complete list of options supported by the Valkey result backend, see
:ref:`conf-valkey-result-backend`.

If you are using Sentinel, you should specify the master_name using the :setting:`result_backend_transport_options` setting:

.. code-block:: python

    app.conf.result_backend_transport_options = {'master_name': "mymaster"}

.. _valkey-result-backend-global-keyprefix:

Global keyprefix
^^^^^^^^^^^^^^^^

The global key prefix will be prepended to all keys used for the result backend,
which can be useful when a valkey database is shared by different users.
By default, no prefix is prepended.

To configure the global keyprefix for the Valkey result backend, use the ``global_keyprefix`` key under :setting:`result_backend_transport_options`:


.. code-block:: python

    app.conf.result_backend_transport_options = {
        'global_keyprefix': 'my_prefix_'
    }

.. _valkey-result-backend-timeout:

Connection timeouts
^^^^^^^^^^^^^^^^^^^

To configure the connection timeouts for the Valkey result backend, use the ``retry_policy`` key under :setting:`result_backend_transport_options`:


.. code-block:: python

    app.conf.result_backend_transport_options = {
        'retry_policy': {
           'timeout': 5.0
        }
    }

See :func:`~kombu.utils.functional.retry_over_time` for the possible retry policy options.

.. _valkey-serverless:

Serverless
==========

Celery supports utilizing a remote serverless Valkey, which can significantly
reduce the operational overhead and cost, making it a favorable choice in
microservice architectures or environments where minimizing operational
expenses is crucial. Serverless Valkey provides the necessary functionalities
without the need for manual setup, configuration, and management, thus
aligning well with the principles of automation and scalability that Celery promotes.

Upstash
-------

`Upstash <http://upstash.com/?code=celery>`_ offers a serverless Valkey database service,
providing a seamless solution for Celery users looking to leverage
serverless architectures. Upstash's serverless Valkey service is designed
with an eventual consistency model and durable storage, facilitated
through a multi-tier storage architecture.

Integration with Celery is straightforward as demonstrated
in an `example provided by Upstash <https://github.com/upstash/examples/tree/main/examples/using-celery>`_.

.. _valkey-caveats:

Caveats
=======

Visibility timeout
------------------

If a task isn't acknowledged within the :ref:`valkey-visibility_timeout`
the task will be redelivered to another worker and executed.

This causes problems with ETA/countdown/retry tasks where the
time to execute exceeds the visibility timeout; in fact if that
happens it will be executed again, and again in a loop.

To remediate that, you can increase the visibility timeout to match
the time of the longest ETA you're planning to use. However, this is not
recommended as it may have negative impact on the reliability.
Celery will redeliver messages at worker shutdown,
so having a long visibility timeout will only delay the redelivery
of 'lost' tasks in the event of a power failure or forcefully terminated
workers.

Broker is not a database, so if you are in need of scheduling tasks for
a more distant future, database-backed periodic task might be a better choice.
Periodic tasks won't be affected by the visibility timeout,
as this is a concept separate from ETA/countdown.

You can increase this timeout by configuring all of the following options
with the same name (required to set all of them):

.. code-block:: python

    app.conf.broker_transport_options = {'visibility_timeout': 43200}
    app.conf.result_backend_transport_options = {'visibility_timeout': 43200}
    app.conf.visibility_timeout = 43200

The value must be an int describing the number of seconds.

Note: If multiple applications are sharing the same Broker, with different settings, the _shortest_ value will be used.
This include if the value is not set, and the default is sent

Soft Shutdown
-------------

During :ref:`shutdown <worker-stopping>`, the worker will attempt to re-queue any unacknowledged messages
with :setting:`task_acks_late` enabled. However, if the worker is terminated forcefully
(:ref:`cold shutdown <worker-cold-shutdown>`), the worker might not be able to re-queue the tasks on time,
and they will not be consumed again until the :ref:`valkey-visibility_timeout` has passed. This creates a
problem when the :ref:`valkey-visibility_timeout` is very high and a worker needs to shut down just after it has
received a task. If the task is not re-queued in such case, it will need to wait for the long visibility timeout
to pass before it can be consumed again, leading to potentially very long delays in tasks execution.

The :ref:`soft shutdown <worker-soft-shutdown>` introduces a time-limited warm shutdown phase just before
the :ref:`cold shutdown <worker-cold-shutdown>`. This time window significantly increases the chances of
re-queuing the tasks during shutdown which mitigates the problem of long visibility timeouts.

To enable the :ref:`soft shutdown <worker-soft-shutdown>`, set the :setting:`worker_soft_shutdown_timeout` to a value
greater than 0. The value must be an float describing the number of seconds. During this time, the worker will
continue to process the running tasks until the timeout expires, after which the :ref:`cold shutdown <worker-cold-shutdown>`
will be initiated automatically to terminate the worker gracefully.

If the :ref:`REMAP_SIGTERM <worker-REMAP_SIGTERM>` is configured to SIGQUIT in the environment variables, and
the :setting:`worker_soft_shutdown_timeout` is set, the worker will initiate the :ref:`soft shutdown <worker-soft-shutdown>`
when it receives the :sig:`TERM` signal (*and* the :sig:`QUIT` signal).

Key eviction
------------

Valkey may evict keys from the database in some situations

If you experience an error like:

.. code-block:: text

    InconsistencyError: Probably the key ('_kombu.binding.celery') has been
    removed from the Valkey database.

then you may want to configure the :command:`valkey-server` to not evict keys
by setting in the valkey configuration file:

- the ``maxmemory`` option
- the ``maxmemory-policy`` option to ``noeviction`` or ``allkeys-lru``

See Valkey server documentation about Eviction Policies for details:

    https://valkey.io/topics/lru-cache

.. _valkey-group-result-ordering:

Group result ordering
---------------------

Versions of Celery up to and including 4.4.6 used an unsorted list to store
result objects for groups in the Valkey backend. This can cause those results to
be be returned in a different order to their associated tasks in the original
group instantiation. Celery 4.4.7 introduced an opt-in behaviour which fixes
this issue and ensures that group results are returned in the same order the
tasks were defined, matching the behaviour of other backends. In Celery 5.0
this behaviour was changed to be opt-out. The behaviour is controlled by the
`result_chord_ordered` configuration option which may be set like so:

.. code-block:: python

    # Specifying this for workers running Celery 4.4.6 or earlier has no effect
    app.conf.result_backend_transport_options = {
        'result_chord_ordered': True    # or False
    }

This is an incompatible change in the runtime behaviour of workers sharing the
same Valkey backend for result storage, so all workers must follow either the
new or old behaviour to avoid breakage. For clusters with some workers running
Celery 4.4.6 or earlier, this means that workers running 4.4.7 need no special
configuration and workers running 5.0 or later must have `result_chord_ordered`
set to `False`. For clusters with no workers running 4.4.6 or earlier but some
workers running 4.4.7, it is recommended that `result_chord_ordered` be set to
`True` for all workers to ease future migration. Migration between behaviours
will disrupt results currently held in the Valkey backend and cause breakage if
downstream tasks are run by migrated workers - plan accordingly.

.. _broker-gcpubsub:

=====================
 Using Google Pub/Sub
=====================

.. versionadded:: 5.5

.. _broker-gcpubsub-installation:

Installation
============

For the Google Pub/Sub support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[gcpubsub]`` :ref:`bundle <bundles>`:

.. code-block:: console

    $ pip install "celery[gcpubsub]"

.. _broker-gcpubsub-configuration:

Configuration
=============

You have to specify gcpubsub and google project in the broker URL::

    broker_url = 'gcpubsub://projects/project-id'

where the URL format is:

.. code-block:: text

    gcpubsub://projects/project-id

Please note that you must prefix the project-id with `projects/` in the URL.

The login credentials will be your regular GCP credentials set in the environment.

Options
=======

Resource expiry
---------------

The default settings are built to be as simple cost effective and intuitive as possible and to "just work".
The pubsub messages and subscriptions are set to expire after 24 hours, and can be set
by configuring the :setting:`expiration_seconds` setting::

    expiration_seconds = 86400

.. seealso::

    An overview of Google Cloud Pub/Sub settings can be found here:

        https://cloud.google.com/pubsub/docs

.. _gcpubsub-ack_deadline_seconds:

Ack Deadline Seconds
--------------------

The `ack_deadline_seconds` defines the number of seconds pub/sub infra shall wait
for the worker to acknowledge the task before the message is redelivered
to another worker.

This option is set via the :setting:`broker_transport_options` setting::

    broker_transport_options = {'ack_deadline_seconds': 60}  # 1 minute.

The default visibility timeout is 240 seconds, and the worker takes care for
automatically extending all pending messages it has.

.. seealso::

    An overview of Pub/Sub deadline can be found here:

        https://cloud.google.com/pubsub/docs/lease-management



Polling Interval
----------------

The polling interval decides the number of seconds to sleep between
unsuccessful polls. This value can be either an int or a float.
By default the value is *0.1 seconds*. However it doesn't mean
that the worker will bomb the Pub/Sub API every 0.1 seconds when there's no
more messages to read, since it will be blocked by a blocking call to
the Pub/Sub API, which will only return when there's a new message to read
or after 10 seconds.

The polling interval can be set via the :setting:`broker_transport_options`
setting::

    broker_transport_options = {'polling_interval': 0.3}

Very frequent polling intervals can cause *busy loops*, resulting in the
worker using a lot of CPU time. If you need sub-millisecond precision you
should consider using another transport, like `RabbitMQ <broker-amqp>`,
or `Redis <broker-redis>`.

Queue Prefix
------------

By default Celery will assign `kombu-` prefix to the queue names,
If you have other services using Pub/Sub you can configure it do so
using the :setting:`broker_transport_options` setting::

    broker_transport_options = {'queue_name_prefix': 'kombu-'}

.. _gcpubsub-results-configuration:

Results
-------

Google Cloud Storage (GCS) could be a good candidate to store the results.
See :ref:`gcs` for more information.


Caveats
=======

- When using celery flower, an --inspect-timeout=10 option is required to
  detect workers state correctly.

- GCP Subscriptions idle subscriptions (no queued messages)
  are configured to removal after 24hrs.
  This aims at reducing costs.

- Queued and unacked messages are set to auto cleanup after 24 hrs.
  Same reason as above.

- Channel queue size is approximation, and may not be accurate.
  The reason is that the Pub/Sub API does not provide a way to get the
  exact number of messages in a subscription.

- Orphan (no subscriptions) Pub/Sub topics aren't being auto removed!!
  Since GCP introduces a hard limit of 10k topics per project,
  it is recommended to remove orphan topics manually in a periodic manner.

- Max message size is limited to 10MB, as a workaround you can use GCS Backend to
  store the message in GCS and pass the GCS URL to the task.

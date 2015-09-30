.. _broker-sqs:

==================
 Using Amazon SQS
==================

.. admonition:: Experimental Status

    The SQS transport is in need of improvements in many areas and there
    are several open bugs.  Unfortunately we don't have the resources or funds
    required to improve the situation, so we're looking for contributors
    and partners willing to help.

.. _broker-sqs-installation:

Installation
============

For the Amazon SQS support you have to install the `boto`_ library:

.. code-block:: console

    $ pip install -U boto

.. _boto:
    http://pypi.python.org/pypi/boto

.. _broker-sqs-configuration:

Configuration
=============

You have to specify SQS in the broker URL::

    BROKER_URL = 'sqs://ABCDEFGHIJKLMNOPQRST:ZYXK7NiynGlTogH8Nj+P9nlE73sq3@'

where the URL format is::

    sqs://aws_access_key_id:aws_secret_access_key@

you must *remember to include the "@" at the end*.

The login credentials can also be set using the environment variables
:envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY`,
in that case the broker url may only be ``sqs://``.

.. note::

    If you specify AWS credentials in the broker URL, then please keep in mind
    that the secret access key may contain unsafe characters that needs to be
    URL encoded.

Options
=======

Region
------

The default region is ``us-east-1`` but you can select another region
by configuring the :setting:`BROKER_TRANSPORT_OPTIONS` setting::

    BROKER_TRANSPORT_OPTIONS = {'region': 'eu-west-1'}

.. seealso::

    An overview of Amazon Web Services regions can be found here:

        http://aws.amazon.com/about-aws/globalinfrastructure/

Visibility Timeout
------------------

The visibility timeout defines the number of seconds to wait
for the worker to acknowledge the task before the message is redelivered
to another worker.  Also see caveats below.

This option is set via the :setting:`BROKER_TRANSPORT_OPTIONS` setting::

    BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 3600}  # 1 hour.

The default visibility timeout is 30 seconds.

Polling Interval
----------------

The polling interval decides the number of seconds to sleep between
unsuccessful polls.  This value can be either an int or a float.
By default the value is 1 second, which means that the worker will
sleep for one second whenever there are no more messages to read.

You should note that **more frequent polling is also more expensive, so increasing
the polling interval can save you money**.

The polling interval can be set via the :setting:`BROKER_TRANSPORT_OPTIONS`
setting::

    BROKER_TRANSPORT_OPTIONS = {'polling_interval': 0.3}

Very frequent polling intervals can cause *busy loops*, which results in the
worker using a lot of CPU time.  If you need sub-millisecond precision you
should consider using another transport, like `RabbitMQ <broker-amqp>`,
or `Redis <broker-redis>`.

Queue Prefix
------------

By default Celery will not assign any prefix to the queue names,
If you have other services using SQS you can configure it do so
using the :setting:`BROKER_TRANSPORT_OPTIONS` setting::

    BROKER_TRANSPORT_OPTIONS = {'queue_name_prefix': 'celery-'}


.. _sqs-caveats:

Caveats
=======

- If a task is not acknowledged within the ``visibility_timeout``,
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
    as it is a concept separate from ETA/countdown.

    The maximum visibility timeout supported by AWS as of this writing
    is 12 hours (43200 seconds)::

        BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 43200}

- SQS does not yet support worker remote control commands.

- SQS does not yet support events, and so cannot be used with
  :program:`celery events`, :program:`celerymon` or the Django Admin
  monitor.

.. _sqs-results-configuration:

Results
-------

Multiple products in the Amazon Web Services family could be a good candidate
to store or publish results with, but there is no such result backend included
at this point.

.. warning::

    Do not use the ``amqp`` result backend with SQS.

    It will create one queue for every task, and the queues will
    not be collected.  This could cost you money that would be better
    spent contributing an AWS result store backend back to Celery :)

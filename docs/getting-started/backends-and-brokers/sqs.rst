.. _broker-sqs:

==================
 Using Amazon SQS
==================

.. _broker-sqs-installation:

Installation
============

For the Amazon SQS support you have to install additional dependencies.
You can install both Celery and these dependencies in one go using
the ``celery[sqs]`` :ref:`bundle <bundles>`:

.. code-block:: console

    $ pip install celery[sqs]

.. _broker-sqs-configuration:

Configuration
=============

You have to specify SQS in the broker URL::

    broker_url = 'sqs://ABCDEFGHIJKLMNOPQRST:ZYXK7NiynGlTogH8Nj+P9nlE73sq3@'

where the URL format is:

.. code-block:: text

    sqs://aws_access_key_id:aws_secret_access_key@

Please note that you must remember to include the ``@`` sign at the end and
encode the password so it can always be parsed correctly. For example:

.. code-block:: python

    from kombu.utils.url import safequote

    aws_access_key = safequote("ABCDEFGHIJKLMNOPQRST")
    aws_secret_key = safequote("ZYXK7NiynG/TogH8Nj+P9nlE73sq3")

    broker_url = "sqs://{aws_access_key}:{aws_secret_key}@".format(
        aws_access_key=aws_access_key, aws_secret_key=aws_secret_key,
    )

.. warning::

    Don't use this setup option with django's ``debug=True``.
    It may lead to security issues within deployed django apps.

    In debug mode django shows environment variables and the SQS URL
    may be exposed to the internet including your AWS access and secret keys.
    Please turn off debug mode on your deployed django application or
    consider a setup option described below.


The login credentials can also be set using the environment variables
:envvar:`AWS_ACCESS_KEY_ID` and :envvar:`AWS_SECRET_ACCESS_KEY`,
in that case the broker URL may only be ``sqs://``.

If you are using IAM roles on instances, you can set the BROKER_URL to:
``sqs://`` and kombu will attempt to retrieve access tokens from the instance
metadata.

Options
=======

Region
------

The default region is ``us-east-1`` but you can select another region
by configuring the :setting:`broker_transport_options` setting::

    broker_transport_options = {'region': 'eu-west-1'}

.. seealso::

    An overview of Amazon Web Services regions can be found here:

        http://aws.amazon.com/about-aws/globalinfrastructure/

Visibility Timeout
------------------

The visibility timeout defines the number of seconds to wait
for the worker to acknowledge the task before the message is redelivered
to another worker. Also see caveats below.

This option is set via the :setting:`broker_transport_options` setting::

    broker_transport_options = {'visibility_timeout': 3600}  # 1 hour.

The default visibility timeout is 30 minutes.

Polling Interval
----------------

The polling interval decides the number of seconds to sleep between
unsuccessful polls. This value can be either an int or a float.
By default the value is *one second*: this means the worker will
sleep for one second when there's no more messages to read.

You must note that **more frequent polling is also more expensive, so increasing
the polling interval can save you money**.

The polling interval can be set via the :setting:`broker_transport_options`
setting::

    broker_transport_options = {'polling_interval': 0.3}

Very frequent polling intervals can cause *busy loops*, resulting in the
worker using a lot of CPU time. If you need sub-millisecond precision you
should consider using another transport, like `RabbitMQ <broker-amqp>`,
or `Redis <broker-redis>`.

Long Polling
------------

`SQS Long Polling`_ is enabled by default and the ``WaitTimeSeconds`` parameter
of `ReceiveMessage`_ operation is set to 10 seconds.

The value of ``WaitTimeSeconds`` parameter can be set via the
:setting:`broker_transport_options` setting::

    broker_transport_options = {'wait_time_seconds': 15}

Valid values are 0 to 20. Note that newly created queues themselves (also if
created by Celery) will have the default value of 0 set for the "Receive Message
Wait Time" queue property.

.. _`SQS Long Polling`: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html
.. _`ReceiveMessage`: https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/API_ReceiveMessage.html

Queue Prefix
------------

By default Celery won't assign any prefix to the queue names,
If you have other services using SQS you can configure it do so
using the :setting:`broker_transport_options` setting::

    broker_transport_options = {'queue_name_prefix': 'celery-'}

Predefined Queues
-----------------

If you want Celery to use a set of predefined queues in AWS, and to
never attempt to list SQS queues, nor attempt to create or delete them,
pass a map of queue names to URLs using the :setting:`predefined_queues`
setting::

    broker_transport_options = {
        'predefined_queues': {
            'my-q': {
                'url': 'https://ap-southeast-2.queue.amazonaws.com/123456/my-q',
                'access_key_id': 'xxx',
                'secret_access_key': 'xxx',
            }
        }
    }

Back-off policy
------------------------
Back-off policy is using SQS visibility timeout mechanism altering the time difference between task retries.
The mechanism changes message specific ``visibility timeout`` from queue ``Default visibility timeout`` to policy configured timeout.
The number of retries is managed by SQS (specifically by the ``ApproximateReceiveCount`` message attribute) and no further action is required by the user.

Configuring the queues and backoff policy::

    broker_transport_options = {
        'predefined_queues': {
            'my-q': {
                'url': 'https://ap-southeast-2.queue.amazonaws.com/123456/my-q',
                'access_key_id': 'xxx',
                'secret_access_key': 'xxx',
                'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640},
                'backoff_tasks': ['svc.tasks.tasks.task1']
            }
        }
    }


``backoff_policy`` dictionary where key is number of retries, and value is delay seconds between retries (i.e
SQS visibility timeout)
``backoff_tasks`` list of task names to apply the above policy

The above policy:

+-----------------------------------------+--------------------------------------------+
| **Attempt**                             | **Delay**                                  |
+-----------------------------------------+--------------------------------------------+
| ``2nd attempt``                         | 20 seconds                                 |
+-----------------------------------------+--------------------------------------------+
| ``3rd attempt``                         | 40 seconds                                 |
+-----------------------------------------+--------------------------------------------+
| ``4th attempt``                         | 80 seconds                                 |
+-----------------------------------------+--------------------------------------------+
| ``5th attempt``                         | 320 seconds                                |
+-----------------------------------------+--------------------------------------------+
| ``6th attempt``                         | 640 seconds                                |
+-----------------------------------------+--------------------------------------------+


STS token authentication
----------------------------

https://docs.aws.amazon.com/cli/latest/reference/sts/assume-role.html

AWS STS authentication is supported by using the ``sts_role_arn`` and ``sts_token_timeout`` broker transport options. ``sts_role_arn`` is the assumed IAM role ARN we use to authorize our access to SQS.
``sts_token_timeout`` is the token timeout, defaults (and minimum) to 900 seconds. After the mentioned period, a new token will be created::

    broker_transport_options = {
        'predefined_queues': {
            'my-q': {
                'url': 'https://ap-southeast-2.queue.amazonaws.com/123456/my-q',
                'access_key_id': 'xxx',
                'secret_access_key': 'xxx',
                'backoff_policy': {1: 10, 2: 20, 3: 40, 4: 80, 5: 320, 6: 640},
                'backoff_tasks': ['svc.tasks.tasks.task1']
            }
        },
    'sts_role_arn': 'arn:aws:iam::<xxx>:role/STSTest', # optional
    'sts_token_timeout': 900 # optional
    }


.. _sqs-caveats:

Caveats
=======

- If a task isn't acknowledged within the ``visibility_timeout``,
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
    as it is a concept separate from ETA/countdown.

    The maximum visibility timeout supported by AWS as of this writing
    is 12 hours (43200 seconds)::

        broker_transport_options = {'visibility_timeout': 43200}

- SQS doesn't yet support worker remote control commands.

- SQS doesn't yet support events, and so cannot be used with
  :program:`celery events`, :program:`celerymon`, or the Django Admin
  monitor.

- With FIFO queues it might be necessary to set additional message properties such as ``MessageGroupId`` and ``MessageDeduplicationId`` when publishing a message.

  Message properties can be passed as keyword arguments to :meth:`~celery.app.task.Task.apply_async`:

  .. code-block:: python

    message_properties = {
        'MessageGroupId': '<YourMessageGroupId>',
        'MessageDeduplicationId': '<YourMessageDeduplicationId>'
    }
    task.apply_async(**message_properties)


.. _sqs-results-configuration:

Results
-------

Multiple products in the Amazon Web Services family could be a good candidate
to store or publish results with, but there's no such result backend included
at this point.

.. warning::

    Don't use the ``amqp`` result backend with SQS.

    It will create one queue for every task, and the queues will
    not be collected. This could cost you money that would be better
    spent contributing an AWS result store backend back to Celery :)

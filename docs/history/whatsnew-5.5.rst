.. _whatsnew-5.5:

=========================================
 What's new in Celery 5.5 (Immunity)
=========================================
:Author: Tomer Nosrati (``tomer.nosrati at gmail.com``).

.. sidebar:: Change history

    What's new documents describe the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible, and reliable distributed programming framework
to process vast amounts of messages, while providing operations with
the tools required to maintain a distributed system with python.

It's a task queue with focus on real-time processing, while also
supporting task scheduling.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

.. note::

    Following the problems with Freenode, we migrated our IRC channel to Libera Chat
    as most projects did.
    You can also join us using `Gitter <https://gitter.im/celery/celery>`_.

    We're sometimes there to answer questions. We welcome you to join.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is **mostly** backward compatible with previous versions
it's important that you read the following section as this release
is a new major version.

This version is officially supported on CPython 3.8, 3.9, 3.10, 3.11, 3.12 and 3.13.
and is also supported on PyPy3.10+.

.. _`website`: https://celery.readthedocs.io

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 3

Preface
=======

.. note::

    **This release contains fixes for many long standing bugs & stability issues.
    We encourage our users to upgrade to this release as soon as possible.**

The 5.5.0 release is a new feature release for Celery.

Releases in the 5.x series are codenamed after songs of `Jon Hopkins <https://en.wikipedia.org/wiki/Jon_Hopkins>`_.
This release has been codenamed `Immunity <https://www.youtube.com/watch?v=Y8eQR5DMous>`_.

From now on we only support Python 3.8 and above.
We will maintain compatibility with Python 3.8 until it's
EOL in 2024.

*— Tomer Nosrati*

Long Term Support Policy
------------------------

We no longer support Celery 4.x as we don't have the resources to do so.
If you'd like to help us, all contributions are welcome.

Celery 5.x **is not** an LTS release. We will support it until the release
of Celery 6.x.

We're in the process of defining our Long Term Support policy.
Watch the next "What's New" document for updates.

Upgrading from Celery 4.x
=========================

Step 1: Adjust your command line invocation
-------------------------------------------

Celery 5.0 introduces a new CLI implementation which isn't completely backwards compatible.

The global options can no longer be positioned after the sub-command.
Instead, they must be positioned as an option for the `celery` command like so::

    celery --app path.to.app worker

If you were using our :ref:`daemonizing` guide to deploy Celery in production,
you should revisit it for updates.

Step 2: Update your configuration with the new setting names
------------------------------------------------------------

If you haven't already updated your configuration when you migrated to Celery 4.0,
please do so now.

We elected to extend the deprecation period until 6.0 since
we did not loudly warn about using these deprecated settings.

Please refer to the :ref:`migration guide <conf-old-settings-map>` for instructions.

Step 3: Read the important notes in this document
-------------------------------------------------

Make sure you are not affected by any of the important upgrade notes
mentioned in the :ref:`following section <v550-important>`.

You should verify that none of the breaking changes in the CLI
do not affect you. Please refer to :ref:`New Command Line Interface <new_command_line_interface>` for details.

Step 4: Migrate your code to Python 3
-------------------------------------

Celery 5.x only supports Python 3. Therefore, you must ensure your code is
compatible with Python 3.

If you haven't ported your code to Python 3, you must do so before upgrading.

You can use tools like `2to3 <https://docs.python.org/3.8/library/2to3.html>`_
and `pyupgrade <https://github.com/asottile/pyupgrade>`_ to assist you with
this effort.

After the migration is done, run your test suite with Celery 5 to ensure
nothing has been broken.

Step 5: Upgrade to Celery 5.5
-----------------------------

At this point you can upgrade your workers and clients with the new version.

.. _v550-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python versions are:

- CPython 3.8
- CPython 3.9
- CPython 3.10
- CPython 3.11
- CPython 3.12
- CPython 3.13
- PyPy3.10 (``pypy3``)

Python 3.8 Support
------------------

Python 3.8 will reach EOL in October, 2024.

Minimum Dependencies
--------------------

Kombu
~~~~~

Starting from Celery v5.5, the minimum required version is Kombu 5.5.

Redis
~~~~~

redis-py 4.5.2 is the new minimum required version.


SQLAlchemy
~~~~~~~~~~

SQLAlchemy 1.4.x & 2.0.x is now supported in Celery v5.5.

Billiard
~~~~~~~~

Minimum required version is now 4.2.1.

Django
~~~~~~

Minimum django version is bumped to v2.2.28.
Also added --skip-checks flag to bypass django core checks.

.. _v550-news:

News
====

Redis Broker Stability Improvements
-----------------------------------

Long-standing disconnection issues with the Redis broker have been identified and
resolved in Kombu 5.5.0. These improvements significantly enhance stability when
using Redis as a broker, particularly in high-throughput environments.

Additionally, the Redis backend now has better exception handling with the new
``exception_safe_to_retry`` feature, which improves resilience during temporary
Redis connection issues. See :ref:`conf-redis-result-backend` for complete
documentation.

``pycurl`` replaced with ``urllib3``
------------------------------------

Replaced the :pypi:`pycurl` dependency with :pypi:`urllib3`.

We're monitoring the performance impact of this change and welcome feedback from users
who notice any significant differences in their environments.

RabbitMQ Quorum Queues Support
------------------------------

Added support for RabbitMQ's new `Quorum Queues <https://www.rabbitmq.com/docs/quorum-queues>`_
feature, including compatibility with ETA tasks. This implementation has some limitations compared
to classic queues, so please refer to the documentation for details.

`Native Delayed Delivery <https://docs.particular.net/transports/rabbitmq/delayed-delivery>`_
is automatically enabled when quorum queues are detected to implement the ETA mechanism.

See :ref:`using-quorum-queues` for complete documentation.

Configuration options:

- :setting:`broker_native_delayed_delivery_queue_type`: Specifies the queue type for
  delayed delivery (default: ``quorum``)
- :setting:`task_default_queue_type`: Sets the default queue type for tasks
  (default: ``classic``)
- :setting:`worker_detect_quorum_queues`: Controls automatic detection of quorum
  queues (default: ``True``)

Soft Shutdown Mechanism
-----------------------

Soft shutdown is a time limited warm shutdown, initiated just before the cold shutdown.
The worker will allow :setting:`worker_soft_shutdown_timeout` seconds for all currently
executing tasks to finish before it terminates. If the time limit is reached, the worker
will initiate a cold shutdown and cancel all currently executing tasks.

This feature is particularly valuable when using brokers with visibility timeout
mechanisms, such as Redis or SQS. It allows the worker enough time to re-queue
tasks that were not completed before exiting, preventing task loss during worker
shutdown.

See :ref:`worker-stopping` for complete documentation on worker shutdown types.

Configuration options:

- :setting:`worker_soft_shutdown_timeout`: Sets the duration in seconds for the soft
  shutdown period (default: ``0.0``, disabled)
- :setting:`worker_enable_soft_shutdown_on_idle`: Controls whether soft shutdown
  should be enabled even when the worker is idle (default: ``False``)

Pydantic Support
----------------

New native support for Pydantic models in tasks. This integration allows you to
leverage Pydantic's powerful data validation and serialization capabilities directly
in your Celery tasks.

Example usage:

.. code-block:: python

    from pydantic import BaseModel
    from celery import Celery

    app = Celery('tasks')

    class ArgModel(BaseModel):
        value: int

    class ReturnModel(BaseModel):
        value: str

    @app.task(pydantic=True)
    def x(arg: ArgModel) -> ReturnModel:
        # args/kwargs type hinted as Pydantic model will be converted
        assert isinstance(arg, ArgModel)

        # The returned model will be converted to a dict automatically
        return ReturnModel(value=f"example: {arg.value}")

See :ref:`task-pydantic` for complete documentation.

Configuration options:

- ``pydantic=True``: Enables Pydantic integration for the task
- ``pydantic_strict=True/False``: Controls whether strict validation is enabled
  (default: ``False``)
- ``pydantic_context={...}``: Provides additional context for validation
- ``pydantic_dump_kwargs={...}``: Customizes serialization behavior

Google Pub/Sub Transport
------------------------

New support for Google Cloud Pub/Sub as a message transport, expanding Celery's
cloud integration options.

See :ref:`broker-gcpubsub` for complete documentation.

For the Google Pub/Sub support you have to install additional dependencies:

.. code-block:: console

    $ pip install "celery[gcpubsub]"

Then configure your Celery application to use the Google Pub/Sub transport:

.. code-block:: python

    broker_url = 'gcpubsub://projects/project-id'

Python 3.13 Support
-------------------

Official support for Python 3.13. All core dependencies have been updated to
ensure compatibility, including Kombu and py-amqp.

This release maintains compatibility with Python 3.8 through 3.13, as well as
PyPy 3.10+.

REMAP_SIGTERM Support
---------------------

The "REMAP_SIGTERM" feature, previously undocumented, has been tested, documented,
and is now officially supported. This feature allows you to remap the SIGTERM
signal to SIGQUIT, enabling you to initiate a soft or cold shutdown using TERM
instead of QUIT.

This is particularly useful in containerized environments where SIGTERM is the
standard signal for graceful termination.

See :ref:`Cold Shutdown documentation <worker-REMAP_SIGTERM>` for more info.

To enable this feature, set the environment variable:

.. code-block:: bash

    export REMAP_SIGTERM="SIGQUIT"

Database Backend Improvements
----------------------------

New ``create_tables_at_setup`` option for the database backend. This option
controls when database tables are created, allowing for non-lazy table creation.

By default (``create_tables_at_setup=True``), tables are created during backend
initialization. Setting this to ``False`` defers table creation until they are
actually needed, which can be useful in certain deployment scenarios where you want
more control over database schema management.

See :ref:`conf-database-result-backend` for complete documentation.

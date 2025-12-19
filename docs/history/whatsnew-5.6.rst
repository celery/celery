.. _whatsnew-5.6:

=========================================
 What's new in Celery 5.6 (Recovery)
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

This version is officially supported on CPython 3.9, 3.10, 3.11, 3.12 and 3.13,
and is also supported on PyPy3.11+.

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

The 5.6.0 release is a new feature release for Celery.

Releases in the 5.x series are codenamed after songs of `Jon Hopkins <https://en.wikipedia.org/wiki/Jon_Hopkins>`_.
This release has been codenamed `Recovery <https://www.youtube.com/watch?v=MaqlsAmlbzo>`_.

This is the last version to support Python 3.9.
Support for Python 3.8 was removed after v5.6.0b1.

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
mentioned in the :ref:`following section <v560-important>`.

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

Step 5: Upgrade to Celery 5.6
-----------------------------

At this point you can upgrade your workers and clients with the new version.

.. _v560-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python versions are:

- CPython 3.9
- CPython 3.10
- CPython 3.11
- CPython 3.12
- CPython 3.13
- PyPy3.11 (``pypy3``)

Python 3.9 Support
------------------

Python 3.9 will reach EOL in October, 2025.

Minimum Dependencies
--------------------

Kombu
~~~~~

Starting from Celery v5.6, the minimum required version is Kombu 5.6.

Redis
~~~~~

redis-py 4.5.2 is the new minimum required version.


SQLAlchemy
~~~~~~~~~~

SQLAlchemy 1.4.x & 2.0.x is now supported in Celery v5.6.

Billiard
~~~~~~~~

Minimum required version is now 4.2.4.

Django
~~~~~~

Minimum django version is bumped to v2.2.28.
Also added --skip-checks flag to bypass django core checks.

.. _v560-news:

News
====

SQS: Reverted to ``pycurl`` from ``urllib3``
--------------------------------------------

The switch from ``pycurl`` to ``urllib3`` for the SQS transport (introduced in
Celery 5.5.0 via Kombu) has been reverted due to critical issues affecting SQS
users.

Security Fix: Broker Credential Leak Prevention
------------------------------------------------

Fixed a security issue where broker URLs containing passwords were being logged
in plaintext by the delayed delivery mechanism. Broker credentials are now
properly sanitized in all log output.

Memory Leak Fixes
-----------------

Two significant memory leaks have been fixed in this release:

**Exception Handling Memory Leak**: Fixed a critical memory leak in task exception
handling that was particularly severe on Python 3.11+ due to enhanced traceback
data. The fix properly breaks reference cycles in tracebacks to allow garbage
collection. This resolves a long-standing issue that caused worker memory to grow
unbounded over time.

**Pending Result Memory Leak**: Fixed a memory leak where ``AsyncResult``
subscriptions were not being cleaned up when results were forgotten. This affected
users who frequently use ``AsyncResult.forget()`` in their workflows.

ETA Task Memory Limit
---------------------

New configuration option to prevent out-of-memory crashes when workers fetch
large numbers of ETA or countdown tasks. Previously, workers could exhaust
available memory when the broker contained many scheduled tasks.

Configuration option:

- :setting:`worker_eta_task_limit`: Sets the maximum number of ETA tasks to hold
  in worker memory at once (default: ``None``, unlimited)

Example usage:

.. code-block:: python

    app.conf.worker_eta_task_limit = 1000

Queue Type Selection for Auto-created Queues
--------------------------------------------

New configuration options allow specifying the queue type and exchange type when
Celery auto-creates missing queues. This is particularly useful for RabbitMQ users
who want to use quorum queues with auto-created queues.

Configuration options:

- :setting:`task_create_missing_queue_type`: Sets the queue type for auto-created
  queues (e.g., ``quorum``, ``classic``)
- :setting:`task_create_missing_queue_exchange_type`: Sets the exchange type for
  auto-created queues

Example usage:

.. code-block:: python

    app.conf.task_create_missing_queue_type = 'quorum'

Django Connection Pool Support
------------------------------

Fixed an issue where Django applications using psycopg3 connection pooling would
experience ``psycopg_pool.PoolTimeout`` errors after worker forks. Celery now
properly closes Django's connection pools before forking, similar to how Django
itself handles this in its autoreload mechanism.

Redis Backend Improvements
--------------------------

**Credential Provider Support**: Added the :setting:`redis_backend_credential_provider`
setting to the Redis backend. This enables integration with AWS ElastiCache using
IAM authentication and other credential provider mechanisms.

**Client Name Support**: Added the :setting:`redis_client_name` setting to the Redis
backend, making it easier to identify Celery connections when monitoring Redis servers.

Cold Shutdown Improvements
--------------------------

Fixed an issue where tasks would incorrectly fail with a timeout error during
cold shutdown. The worker now properly skips timeout failure handling during
the cold shutdown phase, allowing tasks to complete or be properly requeued.

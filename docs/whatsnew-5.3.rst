.. _whatsnew-5.3:

=========================================
 What's new in Celery 5.3 (Emerald Rush)
=========================================
:Author: Asif Saif Uddin (``auvipy at gmail.com``).

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

This version is officially supported on CPython 3.8, 3.9 & 3.10
and is also supported on PyPy3.8+.

.. _`website`: https://docs.celeryq.dev/en/stable/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

.. note::

    **This release contains fixes for many long standing bugs & stability issues.
    We encourage our users to upgrade to this release as soon as possible.**

The 5.3.0 release is a new feature release for Celery.

Releases in the 5.x series are codenamed after songs of `Jon Hopkins <https://en.wikipedia.org/wiki/Jon_Hopkins>`_.
This release has been codenamed `Emerald Rush <https://www.youtube.com/watch?v=4sk0uDbM5lc>`_.

From now on we only support Python 3.8 and above.
We will maintain compatibility with Python 3.8 until it's
EOL in 2024.

*— Asif Saif Uddin*

Long Term Support Policy
------------------------

We no longer support Celery 4.x as we don't have the resources to do so.
If you'd like to help us, all contributions are welcome.

Celery 5.x **is not** an LTS release. We will support it until the release
of Celery 6.x.

We're in the process of defining our Long Term Support policy.
Watch the next "What's New" document for updates.

Wall of Contributors
--------------------

.. note::

    This wall was automatically generated from git history,
    so sadly it doesn't not include the people who help with more important
    things like answering mailing-list questions.

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
mentioned in the :ref:`following section <v500-important>`.

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

After the migration is done, run your test suite with Celery 4 to ensure
nothing has been broken.

Step 5: Upgrade to Celery 5.3
-----------------------------

At this point you can upgrade your workers and clients with the new version.

.. _v530-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python versions are:

- CPython 3.8
- CPython 3.9
- CPython 3.10
- PyPy3.8 7.3.11 (``pypy3``)

Experimental support
~~~~~~~~~~~~~~~~~~~~

Celery supports these Python versions provisionally as they are not production
ready yet:

- CPython 3.11

Quality Improvements and Stability Enhancements
-----------------------------------------------

Celery 5.3 focuses on elevating the overall quality and stability of the project. 
We have dedicated significant efforts to address various bugs, enhance performance,
and make improvements based on valuable user feedback.

Better Compatibility and Upgrade Confidence
-------------------------------------------

Our goal with Celery 5.3 is to instill confidence in users who are currently 
using Celery 4 or older versions. We want to assure you that upgrading to 
Celery 5.3 will provide a more robust and reliable experience.


Dropped support for Python 3.7
------------------------------

Celery now requires Python 3.8 and above.

Python 3.7 will reach EOL in June, 2023.
In order to focus our efforts we have dropped support for Python 3.6 in
this version.

If you still require to run Celery using Python 3.7
you can still use Celery 5.2.
However we encourage you to upgrade to a supported Python version since
no further security patches will be applied for Python 3.7 after
the 23th of June, 2023.


Automatic re-connection on connection loss to broker
----------------------------------------------------

Unless :setting:`broker_connection_retry_on_startup` is set to False,
Celery will automatically retry reconnecting to the broker after 
the first connection loss. :setting:`broker_connection_retry` controls 
whether to automatically retry reconnecting to the broker for subsequent
reconnects.

Since the message broker does not track how many tasks were already fetched 
before the connection was lost, Celery will reduce the prefetch count by 
the number of tasks that are currently running multiplied by 
:setting:`worker_prefetch_multiplier`.
The prefetch count will be gradually restored to the maximum allowed after
each time a task that was running before the connection was lost is complete


Kombu
-----

Starting from v5.3.0, the minimum required version is Kombu 5.3.0.

Redis
-----

redis-py 4.5.x is the new minimum required version.


SQLAlchemy
---------------------

SQLAlchemy 1.4.x & 2.0.x is now supported in celery v5.3


Billiard
-------------------

Minimum required version is now 4.1.0


Deprecate pytz and use zoneinfo
-------------------------------

A switch have been made to zoneinfo for handling timezone data instead of pytz.


Support for out-of-tree worker pool implementations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Prior to version 5.3, Celery had a fixed notion of the worker pool types it supports.
Celery v5.3.0 introduces the the possibility of an out-of-tree worker pool implementation.
This feature ensure that the current worker pool implementations consistently call into
BasePool._get_info(), and enhance it to report the work pool class in use via the 
"celery inspect stats" command. For example:

$ celery -A ... inspect stats
->  celery@freenas: OK
    {
        ...
        "pool": {
           ...
            "implementation": "celery_aio_pool.pool:AsyncIOPool",

It can be used as follows:

    Set the environment variable CELERY_CUSTOM_WORKER_POOL to the name of
    an implementation of :class:celery.concurrency.base.BasePool in the
    standard Celery format of "package:class".

    Select this pool using '--pool custom'.


Signal::``worker_before_create_process``
----------------------------------------

Dispatched in the parent process, just before new child process is created in the prefork pool.
It can be used to clean up instances that don't behave well when forking.

.. code-block:: python
    @signals.worker_before_create_process.connect
    def clean_channels(**kwargs):
        grpc_singleton.clean_channel()


Setting::``beat_cron_starting_deadline``
----------------------------------------

When using cron, the number of seconds :mod:`~celery.bin.beat` can look back
when deciding whether a cron schedule is due. When set to `None`, cronjobs that
are past due will always run immediately.


Redis result backend Global keyprefix
-------------------------------------

The global key prefix will be prepended to all keys used for the result backend,
which can be useful when a redis database is shared by different users.
By default, no prefix is prepended.

To configure the global keyprefix for the Redis result backend, use the 
``global_keyprefix`` key under :setting:`result_backend_transport_options`:


.. code-block:: python
    app.conf.result_backend_transport_options = {
        'global_keyprefix': 'my_prefix_'
    }


Django
------

Minimum django version is bumped to v2.2.28.
Also added --skip-checks flag to bypass django core checks.


Make default worker state limits configurable
---------------------------------------------

Previously, `REVOKES_MAX`, `REVOKE_EXPIRES`, `SUCCESSFUL_MAX` and
`SUCCESSFUL_EXPIRES` were hardcoded in `celery.worker.state`. This 
version introduces `CELERY_WORKER_` prefixed environment variables 
with the same names that allow you to customize these values should
you need to.


Canvas stamping
---------------

The goal of the Stamping API is to give an ability to label the signature 
and its components for debugging information purposes. For example, when 
the canvas is a complex structure, it may be necessary to label some or 
all elements of the formed structure. The complexity increases even more 
when nested groups are rolled-out or chain elements are replaced. In such 
cases, it may be necessary to understand which group an element is a part 
of or on what nested level it is. This requires a mechanism that traverses 
the canvas elements and marks them with specific metadata. The stamping API 
allows doing that based on the Visitor pattern.


Known Issues
------------
Canvas header stamping has issues in a hybrid Celery 4.x. & Celery 5.3.x 
environment and is not safe for production use at the moment.





.. _whatsnew-5.2:

=========================================
 What's new in Celery 5.2 (Dawn Chorus)
=========================================
:Author: Omer Katz (``omer.drow at gmail.com``)

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

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is **mostly** backward compatible with previous versions
it's important that you read the following section as this release
is a new major version.

This version is officially supported on CPython 3.6, 3.7 & 3.8 & 3.9
and is also supported on PyPy3.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

The 5.2.0 release is a new minor release for Celery.

Releases in the 5.x series are codenamed after songs of `Jon Hopkins <https://en.wikipedia.org/wiki/Jon_Hopkins>`_.
This release has been codenamed `Dawn Chorus <https://www.youtube.com/watch?v=bvsZBdo5pEk>`_.

From now on we only support Python 3.7 and above.
We will maintain compatibility with Python 3.7 until it's
EOL in June, 2023.

*— Omer Katz*

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

Step 5: Upgrade to Celery 5.1
-----------------------------

At this point you can upgrade your workers and clients with the new version.

.. _v520-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python versions are:

- CPython 3.7
- CPython 3.8
- CPython 3.9
- PyPy3.7 7.3 (``pypy3``)

Important Notes
---------------

Kombu
~~~~~

Starting from v5.2, the minimum required version is Kombu 5.2.0.

Important Notes From 5.0
------------------------

Dropped support for Python 2.7 & 3.5
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Celery now requires Python 3.6 and above.

Python 2.7 has reached EOL in January 2020.
In order to focus our efforts we have dropped support for Python 2.7 in
this version.

In addition, Python 3.5 has reached EOL in September 2020.
Therefore, we are also dropping support for Python 3.5.

If you still require to run Celery using Python 2.7 or Python 3.5
you can still use Celery 4.x.
However we encourage you to upgrade to a supported Python version since
no further security patches will be applied for Python 2.7 or
Python 3.5.

Eventlet Workers Pool
~~~~~~~~~~~~~~~~~~~~~

Due to `eventlet/eventlet#526 <https://github.com/eventlet/eventlet/issues/526>`_
the minimum required version is eventlet 0.26.1.

Gevent Workers Pool
~~~~~~~~~~~~~~~~~~~

Starting from v5.0, the minimum required version is gevent 1.0.0.

Couchbase Result Backend
~~~~~~~~~~~~~~~~~~~~~~~~

The Couchbase result backend now uses the V3 Couchbase SDK.

As a result, we no longer support Couchbase Server 5.x.

Also, starting from v5.0, the minimum required version
for the database client is couchbase 3.0.0.

To verify that your Couchbase Server is compatible with the V3 SDK,
please refer to their `documentation <https://docs.couchbase.com/python-sdk/3.0/project-docs/compatibility.html>`_.

Riak Result Backend
~~~~~~~~~~~~~~~~~~~

The Riak result backend has been removed as the database is no longer maintained.

The Python client only supports Python 3.6 and below which prevents us from
supporting it and it is also unmaintained.

If you are still using Riak, refrain from upgrading to Celery 5.0 while you
migrate your application to a different database.

We apologize for the lack of notice in advance but we feel that the chance
you'll be affected by this breaking change is minimal which is why we
did it.

AMQP Result Backend
~~~~~~~~~~~~~~~~~~~

The AMQP result backend has been removed as it was deprecated in version 4.0.

Removed Deprecated Modules
~~~~~~~~~~~~~~~~~~~~~~~~~~

The `celery.utils.encoding` and the `celery.task` modules has been deprecated
in version 4.0 and therefore are removed in 5.0.

If you were using the `celery.utils.encoding` module before,
you should import `kombu.utils.encoding` instead.

If you were using the `celery.task` module before, you should import directly
from the `celery` module instead.

`azure-servicebus` 7.0.0 is now required
----------------------------------------

Given the SDK changes between 0.50.0 and 7.0.0 Kombu deprecates support for
older `azure-servicebus` versions.

.. _v520-news:

News
====


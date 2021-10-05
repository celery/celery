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

This version is officially supported on CPython 3.7 & 3.8 & 3.9
and is also supported on PyPy3.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

.. note::

    **This release contains fixes for two (potentially severe) memory leaks.
    We encourage our users to upgrade to this release as soon as possible.**

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

Step 5: Upgrade to Celery 5.2
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

Experimental support
~~~~~~~~~~~~~~~~~~~~

Celery supports these Python versions provisionally as they are not production
ready yet:

- CPython 3.10 (currently in RC2)

Memory Leak Fixes
-----------------

Two severe memory leaks have been fixed in this version:

* :class:`celery.result.ResultSet` no longer holds a circular reference to itself.
* The prefork pool no longer keeps messages in its cache forever when the master
  process disconnects from the broker.

The first memory leak occurs when you use :class:`celery.result.ResultSet`.
Each instance held a promise which provides that instance as an argument to
the promise's callable.
This caused a circular reference which kept the ResultSet instance in memory
forever since the GC couldn't evict it.
The provided argument is now a :func:`weakref.proxy` of the ResultSet's
instance.
The memory leak mainly occurs when you use :class:`celery.result.GroupResult`
since it inherits from :class:`celery.result.ResultSet` which doesn't get used
that often.

The second memory leak exists since the inception of the project.
The prefork pool maintains a cache of the jobs it executes.
When they are complete, they are evicted from the cache.
However, when Celery disconnects from the broker, we flush the pool
and discard the jobs, expecting that they'll be cleared later once the worker
acknowledges them but that has never been the case.
Instead, these jobs remain forever in memory.
We now discard those jobs immediately while flushing.

Dropped support for Python 3.6
------------------------------

Celery now requires Python 3.7 and above.

Python 3.6 will reach EOL in December, 2021.
In order to focus our efforts we have dropped support for Python 3.6 in
this version.

If you still require to run Celery using Python 3.6
you can still use Celery 5.1.
However we encourage you to upgrade to a supported Python version since
no further security patches will be applied for Python 3.6 after
the 23th of December, 2021.

Tasks
-----

When replacing a task with another task, we now give an indication of the
replacing nesting level through the ``replaced_task_nesting`` header.

A task which was never replaced has a ``replaced_task_nesting`` value of 0.

Kombu
-----

Starting from v5.2, the minimum required version is Kombu 5.2.0.

Prefork Workers Pool
---------------------

Now all orphaned worker processes are killed automatically when main process exits.

Eventlet Workers Pool
---------------------

You can now terminate running revoked tasks while using the
Eventlet Workers Pool.

Custom Task Classes
-------------------

We introduced a custom handler which will be executed before the task
is started called ``before_start``.

See :ref:`custom-task-cls-app-wide` for more details.

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

Support for invoking chords of unregistered tasks
-------------------------------------------------

Previously if you attempted to publish a chord
while providing a signature which wasn't registered in the Celery app publishing
the chord as the body of the chord, an :exc:`celery.exceptions.NotRegistered`
exception would be raised.

From now on, you can publish these sort of chords and they would be executed
correctly:

.. code-block:: python

    # movies.task.publish_movie is registered in the current app
    movie_task = celery_app.signature('movies.task.publish_movie', task_id=str(uuid.uuid4()), immutable=True)
    # news.task.publish_news is *not* registered in the current app
    news_task = celery_app.signature('news.task.publish_news', task_id=str(uuid.uuid4()), immutable=True)

    my_chord = chain(movie_task,
                     group(movie_task.set(task_id=str(uuid.uuid4())),
                           movie_task.set(task_id=str(uuid.uuid4()))),
                     news_task)
    my_chord.apply_async()  # <-- No longer raises an exception

Consul Result Backend
---------------------

We now create a new client per request to Consul to avoid a bug in the Consul
client.

The Consul Result Backend now accepts a new
:setting:`result_backend_transport_options` key: ``one_client``.
You can opt out of this behavior by setting ``one_client`` to True.

Please refer to the documentation of the backend if you're using the Consul
backend to find out which behavior suites you.

Filesystem Result Backend
-------------------------

We now cleanup expired task results while using the
filesystem result backend as most result backends do.

ArangoDB Result Backend
-----------------------

You can now check the validity of the CA certificate while making
a TLS connection to ArangoDB result backend.

If you'd like to do so, set the ``verify`` key in the
:setting:`arangodb_backend_settings`` dictionary to ``True``.

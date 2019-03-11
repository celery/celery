.. _whatsnew-4.3:

===================================
 What's new in Celery 4.3 (rhubarb)
===================================
:Author: Omer Katz (``omer.drow at gmail.com``)

.. sidebar:: Change history

    What's new documents describe the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible, and reliable distributed system to
process vast amounts of messages, while providing operations with
the tools required to maintain such a system.

It's a task queue with focus on real-time processing, while also
supporting task scheduling.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is backward compatible with previous versions
it's important that you read the following section.

This version is officially supported on CPython 2.7, 3.4, 3.5, 3.6 & 3.7
and is also supported on PyPy2 & PyPy3.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

The 4.3.0 release continues to improve our efforts to provide you with
the best task execution platform for Python.

This release has been codenamed `Rhubarb <https://www.youtube.com/watch?v=_AWIqXzvX-U>`_ which is one of my favorite tracks from
Selected Ambient Works II.

This release focuses on new features like new result backends
and a revamped security serializer along with bug fixes mainly for Celery Beat,
Canvas, a number of critical fixes for hanging workers and
fixes for several severe memory leaks.

Celery 4.3 is the first release to support Python 3.7.

We hope that 4.3 will be the last release to support Python 2.7 as we now
begin to work on Celery 5, the next generation of our task execution platform.

However, if Celery 5 will be delayed for any reason we may release
another 4.x minor version which will still support Python 2.7.

If another 4.x version will be released it will most likely drop support for
Python 3.4 as it will reach it's EOL in March 2019.

We have also focused on reducing contribution friction.

Thanks to **Josue Balandrano Coronel**, one of our core contributors, we now have an
updated :ref:`contributing` document.
If you intend to contribute, please review it at your earliest convenience.

I have also added new issue templates, which we will continue to improve,
so that the issues you open will have more relevant information which
will allow us to help you to resolve them more easily.

*— Omer Katz*

Wall of Contributors
--------------------

.. note::

    This wall was automatically generated from git history,
    so sadly it doesn't not include the people who help with more important
    things like answering mailing-list questions.


Upgrading from Celery 4.2
=========================

Please read the important notes below as there are several breaking changes.

.. _v430-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python Versions are:

- CPython 2.7
- CPython 3.4
- CPython 3.5
- CPython 3.6
- CPython 3.7
- PyPy2.7 6.0 (``pypy2``)
- PyPy3.5 6.0 (``pypy3``)

Kombu
-----

Starting from this release, the minimum required version is Kombu 4.4.

New Compression Algorithms
~~~~~~~~~~~~~~~~~~~~~~~~~~

Kombu 4.3 includes a few new optional compression methods:

- LZMA (available from stdlib if using Python 3 or from a backported package)
- Brotli (available if you install either the brotli or the brotlipy package)
- ZStandard (available if you install the zstandard package)

Unfortunately our current protocol generates huge payloads for complex canvases.

Until we migrate to our 3rd revision of the Celery protocol in Celery 5
which will resolve this issue, please use one of the new compression methods
as a workaround.

See :ref:`calling-compression` for details.

Billiard
--------

Starting from this release, the minimum required version is Billiard 3.6.

Redis Message Broker
--------------------

Due to multiple bugs in earlier versions of py-redis that were causing
issues for Celery, we were forced to bump the minimum required version to 3.2.0.

Redis Result Backend
--------------------

Due to multiple bugs in earlier versions of py-redis that were causing
issues for Celery, we were forced to bump the minimum required version to 3.2.0.

Riak Result Backend
--------------------

The official Riak client does not support Python 3.7 as of yet.

In case you are using the Riak result backend, either attempt to install the
client from master or avoid upgrading to Python 3.7 until this matter is resolved.

In case you are using the Riak result backend with Python 3.7, we now emit
a warning.

Please track `basho/riak-python-client#534 <https://github.com/basho/riak-python-client/issues/534>`_
for updates.

Dropped Support for RabbitMQ 2.x
--------------------------------

Starting from this release, we officially no longer support RabbitMQ 2.x.

The last release of 2.x was in 2012 and we had to make adjustments to
correctly support high availability on RabbitMQ 3.x.

If for some reason, you are still using RabbitMQ 2.x we encourage you to upgrade
as soon as possible since security patches are no longer applied on RabbitMQ 2.x.

Django Support
--------------

Starting from this release, the minimum required Django version is 1.11.

Revamped auth Serializer
------------------------

The auth serializer received a complete overhaul.
It was previously horribly broken.

We now depend on `cryptography` instead of `pyOpenSSL` for this serializer.

See :ref:`message-signing` for details.

.. _v430-news:

News
====

Brokers
-------

Redis Broker Support for SSL URIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Redis broker now has support for SSL connections.

You can use :setting:`broker_use_ssl` as you normally did and use a
`rediss://` URI.

You can also pass the SSL configuration parameters to the URI:

  `rediss://localhost:3456?ssl_keyfile=keyfile.key&ssl_certfile=certificate.crt&ssl_ca_certs=ca.pem&ssl_cert_reqs=CERT_REQUIRED`

Configurable Events Exchange Name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, the events exchange name was hardcoded.

You can use :setting:`event_exchange` to determine it.
The default value remains the same.

Configurable Pidbox Exchange Name
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, the Pidbox exchange name was hardcoded.

You can use :setting:`control_exchange` to determine it.
The default value remains the same.

Result Backends
---------------

Redis Result Backend Support for SSL URIs
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Redis result backend now has support for SSL connections.

You can use :setting:`redis_backend_use_ssl` to configure it and use a
`rediss://` URI.

You can also pass the SSL configuration parameters to the URI:

  `rediss://localhost:3456?ssl_keyfile=keyfile.key&ssl_certfile=certificate.crt&ssl_ca_certs=ca.pem&ssl_cert_reqs=CERT_REQUIRED`


Store Extended Task Metadata in Result
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When :setting:`result_extended` is `True` the backend will store the following
metadata:

- Task Name
- Arguments
- Keyword arguments
- The worker the task was executed on
- Number of retries
- The queue's name or routing key

In addition, :meth:`celery.app.task.update_state` now accepts keyword arguments
which allows you to store custom data with the result.

Encode Results Using A Different Serializer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The :setting:`result_accept_content` setting allows to configure different
accepted content for the result backend.

A special serializer (`auth`) is used for signed messaging,
however the result_serializer remains in json, because we don't want encrypted
content in our result backend.

To accept unsigned content from the result backend,
we introduced this new configuration option to specify the
accepted content from the backend.

New Result Backends
~~~~~~~~~~~~~~~~~~~

This release introduces four new result backends:

  - S3 result backend
  - ArangoDB result backend
  - Azure Block Blob Storage result backend
  - CosmosDB result backend

S3 Result Backend
~~~~~~~~~~~~~~~~~

Amazon Simple Storage Service (Amazon S3) is an object storage service by AWS.

The results are stored using the following path template:

  <:setting:`s3_bucket`>/<:setting:`s3_base_path`>/<key>

See :ref:`conf-s3-result-backend` for more information.

ArangoDB Result Backend
~~~~~~~~~~~~~~~~~~~~~~~

ArangoDB is a native multi-model database with search capabilities.
The backend stores the result in the following document format:

  {
    _key: {key},
    task: {task}
  }

See :ref:`conf-arangodb-result-backend` for more information.

Azure Block Blob Storage Result Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Azure Block Blob Storage is an object storage service by Microsoft.

The backend stores the result in the following path template:

  <:setting:`azureblockblob_container_name`>/<key>

See :ref:`conf-azureblockblob-result-backend` for more information.

CosmosDB Result Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Azure Cosmos DB is Microsoft's globally distributed,
multi-model database service.

The backend stores the result in the following document format:

  {
    id: {key},
    value: {task}
  }

See :ref:`conf-cosmosdbsql-result-backend` for more information.

Tasks
-----

Cythonized Tasks
~~~~~~~~~~~~~~~~

Cythonized tasks are now supported.
You can generate C code from Cython that specifies a task using the `@task`
decorator and everything should work exactly the same.

Acknowledging Tasks on Failures or Timeouts
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When :setting:`task_acks_late` is set to `True` tasks are acknowledged on failures or
timeouts.
This makes it hard to use dead letter queues and exchanges.

Celery 4.3 introduces the new :setting:`task_acks_on_failure_or_timeout` which
allows you to avoid acknowledging tasks if they failed or timed out even if
:setting:`task_acks_late` is set to `True`.

:setting:`task_acks_on_failure_or_timeout` is set to `True` by default.

Schedules Now Support Microseconds
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When scheduling tasks using :program:`celery beat` microseconds
are no longer ignored.

Default Task Priority
~~~~~~~~~~~~~~~~~~~~~

You can now set the default priority of a task using
the :setting:`task_default_priority` setting.
The setting's value will be used if no priority is provided for a specific
task.

Tasks Optionally Inherit Parent's Priority
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Setting the :setting:`task_inherit_parent_priority` configuration option to
`True` will make Celery tasks inherit the priority of the previous task
linked to it.

Examples:

.. code-block:: python

  c = celery.chain(
    add.s(2), # priority=None
    add.s(3).set(priority=5), # priority=5
    add.s(4), # priority=5
    add.s(5).set(priority=3), # priority=3
    add.s(6), # priority=3
  )

.. code-block:: python

  @app.task(bind=True)
  def child_task(self):
    pass

  @app.task(bind=True)
  def parent_task(self):
    child_task.delay()

  # child_task will also have priority=5
  parent_task.apply_async(args=[], priority=5)

Canvas
------

Chords can be Executed in Eager Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When :setting:`task_always_eager` is set to `True`, chords are executed eagerly
as well.

Configurable Chord Join Timeout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Previously, :meth:`celery.result.GroupResult.join` had a fixed timeout of 3
seconds.

The :setting:`result_chord_join_timeout` setting now allows you to change it.

The default remains 3 seconds.

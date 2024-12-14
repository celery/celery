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

Celery v5.5 will be the last version to support Python 3.8.

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

The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer
additional improvements.

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that
are already running. After the soft shutdown ends, the worker will initiate a graceful cold shutdown,
stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option
:setting:`worker_soft_shutdown_timeout`. If a worker is not running any task when the soft shutdown initiates,
it will skip the warm shutdown period and proceed directly to the cold shutdown unless the new configuration option
:setting:`worker_enable_soft_shutdown_on_idle` is set to ``True``. This is useful for workers that are idle,
waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism,
like :ref:`Redis <broker-redis>` or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure,
allowing the worker enough time to re-queue tasks that were not completed (e.g., ``Restoring 1 unacknowledged message(s)``)
by resetting the visibility timeout of the unacknowledged messages just before the worker exits completely.

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks by @mathiasertl:

.. code-block:: bash

    pip install "celery[pydantic]"

You can use `Pydantic <https://docs.pydantic.dev/>`_ to validate and convert arguments as well as serializing
results based on typehints by passing ``pydantic=True``. For example:

.. code-block:: python

    from pydantic import BaseModel

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

The task can then be called using a dict matching the model, and you'll receive
the returned model "dumped" (serialized using ``BaseModel.model_dump()``):

.. code-block:: python

   >>> result = x.delay({'value': 1})
   >>> result.get(timeout=1)
   {'value': 'example: 1'}

There are a few more options influencing Pydantic behavior:

.. attribute:: Task.pydantic_strict

   By default, `strict mode <https://docs.pydantic.dev/dev/concepts/strict_mode/>`_
   is enabled. You can pass ``False`` to disable strict model validation.

.. attribute:: Task.pydantic_context

   Pass `additional validation context
   <https://docs.pydantic.dev/dev/concepts/validators/#validation-context>`_ during
   Pydantic model validation. The context already includes the application object as
   ``celery_app`` and the task name as ``celery_task_name`` by default.

.. attribute:: Task.pydantic_dump_kwargs

   When serializing a result, pass these additional arguments to ``dump_kwargs()``.
   By default, only ``mode='json'`` is passed.

Quorum Queues Initial Support
-----------------------------

This release introduces the initial support for Quorum Queues with Celery.
See the documentation for :ref:`using-quorum-queues` for more details.

In addition, you can read about the new configuration options relevant for this feature:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`
- :setting:`broker_native_delayed_delivery_queue_type`

REMAP_SIGTERM
-------------

The REMAP_SIGTERM "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using TERM
instead of QUIT.

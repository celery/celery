.. _whatsnew-5.7:

=========================================
 What's new in Celery 5.7 (Collider)
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
you should come join us :ref:`on Discord Chanel <https://discord.com/invite/9nhyjpSF>`
or :ref:`our Discussion forum <https://github.com/celery/celery/discussions>`.

.. note::

    Following the problems with Freenode, we migrated our IRC channel to Libera Chat
    as most projects did. But this is less used now a days. It is better to use dicord.
    You can also join us using `Gitter <https://gitter.im/celery/celery>`_.

    We're sometimes there to answer questions. We welcome you to join.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is **mostly** backward compatible with previous versions
it's important that you read the following section as this release
is a new major version.

This version is officially supported on CPython 3.10, 3.11, 3.12, 3.13 and 3.14,
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

The 5.7.0 release is a new feature release for Celery.

Releases in the 5.x series are codenamed after songs of `Jon Hopkins <https://en.wikipedia.org/wiki/Jon_Hopkins>`_.
This release has been codenamed `Collider <https://www.youtube.com/watch?v=8XtqUOegCv8>`_.

This is the last version to support Python 3.11.


*— Asif Saif Uddin*


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
mentioned in the :ref:`following section <v570-important>`.

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

Step 5: Upgrade to Celery 5.7
-----------------------------

At this point you can upgrade your workers and clients with the new version.

.. _v570-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python versions are:

- CPython 3.11
- CPython 3.12
- CPython 3.13
- CPython 3.14
- CPython 3.15
- PyPy3.11 (``pypy3``)

Python 3.10 Support
------------------

Python 3.10 will reach EOL in October, 2026.

Minimum Dependencies
--------------------

Kombu
~~~~~

Starting from Celery v5.7, the minimum required version is Kombu 5.7.

Redis
~~~~~

redis-py 6.4.0 is the new minimum required version.
redis server 6.2.0 is the new minimum required version.


SQLAlchemy
~~~~~~~~~~

SQLAlchemy 1.4.x to 2.1.x is now supported in Celery v5.7.

Billiard
~~~~~~~~

Minimum required version is now 4.3.0+.

Django
~~~~~~

Minimum django version is bumped to Django>=3.2.1.


.. _v570-news:

What's New in Celery 5.7
==========================

Celery 5.7 introduces significant improvements to task execution, workflow composition, worker reliability, scheduling, result handling, and operational tooling. This release focuses on making complex distributed task workflows more predictable, improving performance and resource management, and addressing correctness issues across workers, brokers, and result backends.

The highlights include more reliable Canvas execution, improved worker process management and concurrency support, stronger task-state consistency, enhanced timezone and scheduling behavior, and additional result backend capabilities. Celery 5.7 also brings improvements to task publishing, configuration, diagnostics, security, and developer tooling.

The following sections summarize the major changes in this release.

Canvas, Chains, Groups, and Chords
==================================

Celery 5.7 includes substantial improvements to Canvas execution, particularly around chains, groups, chords, and nested workflows.

* **Improved chained failure propagation:** Failures in chained tasks are propagated correctly to chord bodies, improving error handling in workflows that combine chains and chords.
* **More reliable chord execution:** Chords handle failed headers, revoked tasks, and header failure propagation more consistently, reducing the risk of workflows becoming stuck.
* **Improved empty group and chain handling:** Empty groups and chains are handled more consistently in normal and eager execution, including nested workflows and chord headers.
* **More efficient chain construction:** Chains are constructed in a single pass instead of repeatedly folding signatures with the ``|`` operator.
* **Reduced Canvas message growth:** Improvements to standalone chord bodies, implicit chords, and chains of groups prevent unnecessary or exponential growth in serialized Canvas messages.
* **Improved nested group behavior:** Nested groups retain their own identifiers, group ordering is preserved, and result representations handle task names and arguments more consistently.
* **Improved eager execution:** Eager groups avoid unnecessary result-backend access, and chain links are fired only once at the appropriate point in eager execution.
* **Improved signature handling:** Cloning, deep-copying, errback classification, stamping, and nested signature execution handle arguments and options more reliably.
* **Improved task replacement:** Replacing tasks with chains or groups is more reliable, including callback and chain-link handling.

These changes make complex Canvas workflows more predictable and reduce overhead in applications that construct large task graphs.

Worker Concurrency and Process Management
=========================================

Celery 5.7 improves worker process management, pool behavior, and concurrency handling.

* **Spawn-based asynchronous prefork support:** The asynchronous prefork pool can now be used with ``worker_pool_start_method='spawn'``, improving compatibility with environments where process spawning is preferred or required.
* **Improved automatic concurrency detection:** ``--concurrency=auto`` takes cgroup CPU limits into account, allowing workers running in containers to select a more appropriate concurrency level.
* **Configurable worker-loss retries:** The new ``task_retry_on_worker_lost`` setting allows applications to limit task requeues after worker loss.
* **Improved pool lifecycle handling:** Fixes address spawned child detection, worker shutdown, exited child processes, cancellation behavior, and prefork result handling.
* **More reliable autoscaling:** Autoscale keepalive and capacity handling have been improved, including for immediate tasks and green pools.
* **Improved worker initialization:** Worker initialization avoids redundant default configuration setup, and worker process initialization signals receive the worker hostname as their sender.
* **Improved worker shutdown:** Shutdown handling is more reliable, including when logging or signal dispatch raises exceptions.
* **Improved thread pool shutdown:** Pending futures are cancelled during warm shutdown, allowing running tasks to finish without waiting for queued tasks to execute.

Together, these changes improve worker reliability in containerized and multiprocess deployments.

Worker Operations and Remote Control
====================================

Worker management, remote control, and event handling receive several improvements.

* **Beat remote-control inspection:** ``celery beat`` supports opt-in remote-control inspection through the ``ping`` command.
* **Improved event dispatching:** Event dispatcher heartbeat handling, publishing synchronization, and shutdown behavior have been improved.
* **More reliable event buffering:** Buffered events are flushed more consistently, including during broker reconnection and dispatcher shutdown.
* **Improved consumer operation ordering:** Pending consumer operations retain their expected order.
* **Improved reconnect behavior:** Consumer reconnection and prefetch reduction are more reliable, including when using green pools.
* **Improved connection cleanup:** Worker and Beat connection resources are cleaned up more consistently during shutdown and connection loss.
* **Improved remote-control and worker state handling:** Consumer resets, queue selection, and worker state management behave more consistently after connection interruptions.

Task Publishing, Execution, and Request Context
===============================================

Task publishing and execution receive several correctness, reliability, and observability improvements.

* **Merged retry policy for task publishing:** The retry policy is consistently applied to ``before_task_publish`` and task-sent event publishing.
* **Improved protocol compatibility:** Protocol v1 ETA and expiration values are handled without attempting to convert existing string values through ``isoformat()``.
* **Improved task execution context:** The request context is supplied to ``task_failure`` when a worker is lost.
* **Task runtime in success signals:** The task-success signal includes runtime information.
* **Safer execution options:** ``apply_async()`` no longer mutates cached execution options, and task execution copies only declared stamps.
* **Improved task retry behavior:** Retry argument conflicts are handled more consistently when switching calling conventions, and retry options are preserved correctly.
* **Improved task result defaults:** ``send_task()`` and ``Task.apply()`` respect ``ignore_result`` defaults and explicit overrides more consistently.
* **Improved callback handling:** Callback publishing retries more reliably when the broker refuses a publish, and replaced tasks execute their callbacks correctly.
* **Improved task state consistency:** Tasks are no longer incorrectly marked ``REVOKED`` while another worker is executing them. Previously stored successful results are preserved when later worker-loss or callback events occur.
* **Improved task publishing signals:** Signal dispatch is no longer unnecessarily gated on cached receiver lists.

Pydantic Integration
====================

Celery 5.7 extends the existing Pydantic task integration.

Tasks configured with ``pydantic=True`` can now accept Pydantic model instances when called, in addition to the existing supported validation and serialization behavior.

Compatibility with future annotations has also been improved.

Result Backend Improvements
===========================

Celery 5.7 includes improvements across several result backends, including new backend support, more reliable result handling, and better cleanup behavior.

* **New pymemcache backend:** A result backend using ``pymemcache`` is now available.
* **Redis chord expiration:** The Redis result backend adds ``result_chord_expires`` to control the expiration of chord-related keys independently.
* **Redis failure expiration:** The ``result_failure_expires`` setting allows separate expiration configuration for Redis chord failure keys.
* **Improved Redis chord handling:** Chord header tasks are counted only once, including when tasks are redelivered or retried after worker loss. Redis chord claim cleanup is also more reliable.
* **Improved result cleanup:** Several backends handle missing documents, repeated ``forget()`` calls, and result updates more consistently.
* **Improved CosmosDB handling:** Result updates use upserts to ensure later task states are stored correctly.
* **Improved Redis pub/sub handling:** Locking and cancellation behavior have been improved to avoid deadlocks and contention.
* **Improved database backend serialization:** The database backend honors ``result_serializer``.
* **Extended result metadata:** Database-backed results can store task children and stamping metadata when extended results are enabled.
* **Improved result expiration:** Cache backend chords can remain available beyond the regular ``result_expires`` period.
* **More reliable result retrieval:** Nested native results preserve propagation settings, and eager results retain their names when copied or pickled.
* **Improved result consistency:** The database, MongoDB, and Cassandra backends preserve the first successful task result instead of overwriting it with later state updates.
* **Improved result cleanup across timezones:** MongoDB and database backend cleanup handle daylight-saving transitions and timezone configuration more consistently.
* **Improved waiting result handling:** Backend result tracking handles multiple ``AsyncResult`` instances waiting on the same task ID more reliably.

Scheduling, ETA, Expiration, and Celery Beat
============================================

Scheduling and timezone handling have received a broad set of fixes and improvements.

* **Improved daylight-saving handling:** Aware datetimes are normalized into the schedule timezone, and DST gaps and transitions are handled more consistently.
* **Improved crontab calculations:** Crontab scheduling handles non-uniform schedules, minute-slot calculations, persisted state, and impossible dates more reliably.
* **Improved Beat scheduling:** Beat heap management and re-scheduling behavior have been corrected to avoid unnecessary delays and incorrect retry timing.
* **Improved relative schedules:** Relative schedule rounding is more reliable across DST transitions.
* **Improved ETA and expiration handling:** Task ETA, countdown, and expiration calculations are more consistent across timezone transitions.
* **Improved Beat task metadata:** Task options can include the ``celery_beat_task`` header to identify tasks dispatched by Beat.
* **Crontab starting deadline:** The ``beat_cron_starting_deadline`` setting is handled correctly for non-uniform crontab schedules.
* **More resilient Beat operation:** Beat continues running when a schedule entry fails instead of terminating unexpectedly.
* **Improved solar scheduling:** Solar schedule timing is more reliable across daylight-saving transitions.

Queue Management and Routing
============================

Queue selection, aliases, and routing behavior have been improved.

* **Queue aliases:** Queue aliases are supported by ``Queues.deselect()``, and alias-based selection consistently resolves to the actual queue name.
* **Default routing key:** Queues receive the configured default routing key when one is not explicitly provided.
* **Default maximum priority:** Queue declarations apply the default maximum priority when the queue does not specify one.
* **Improved consumer queue selection:** Routing-only queues are no longer unintentionally consumed, and adding known consumers restores the expected queue selection.
* **Improved default queue resolution:** Explicitly routed tasks defer default queue lookup, avoiding unnecessary default queue resolution.
* **Updated AMQP routing:** Task routes are refreshed consistently across configuration update mechanisms.
* **Improved broker compatibility:** RabbitMQ 4.3 compatibility handling for transient queues has been refined.

Configuration and Application Behavior
======================================

The configuration system and application initialization have received several correctness improvements.

* **Improved ``ConfigurationView``:** Membership checks, mapping order, key lookup, and ``swap_with()`` behave more consistently.
* **Improved lazy configuration loading:** ``config_from_object(silent=True)`` is respected during lazy configuration loading.
* **Safer configuration defaults:** Preconfigured values are kept separate from configuration defaults, and configuration copying preserves existing settings.
* **Backend resolution:** ``backends.by_url()`` accepts backend classes, making custom backend resolution more flexible.
* **Django integration:** Django task-class selection has been improved for Celery subclasses without a custom task class.
* **Improved application behavior:** Several initialization and configuration edge cases have been addressed to make application setup more predictable.
* **Improved task registration diagnostics:** Applying ``@shared_task`` twice now raises a clearer error.

Logging, Diagnostics, and Security
==================================

Celery 5.7 improves diagnostic output, logging configuration, and credential protection.

* **Task representation depth:** The new ``task_repr_maxlevels`` setting controls the depth used when representing task arguments and keyword arguments.
* **Logging date formats:** ``ColorFormatter`` and ``TaskFormatter`` support ``datefmt``.
* **Credential masking:** Broker read and write URLs, including credentials in alternate broker URLs, are masked in diagnostic and inspection output.
* **Azure credential redaction:** SAS tokens and account keys are redacted from Azure Block Blob backend diagnostic output.
* **Improved URL sanitization:** Multi-server URLs are handled more consistently by URL sanitization and bug-report utilities.
* **Improved diagnostic formatting:** ``saferepr`` now preserves balanced closing brackets when truncating representations, improving the readability of task arguments, keyword arguments, and logged results.
* **Improved retry logging:** Connection retry tracebacks are logged at debug level, reducing unnecessary noise in normal operation.

Command-Line and Developer Experience
=====================================

Several command-line and developer-facing improvements make Celery easier to operate and maintain.

* **Improved ``celery multi`` index handling:** Index lists such as ``-c:1,2`` are applied correctly to named nodes.
* **Improved spawned worker working directories:** ``~`` is expanded in ``--workdir`` when starting spawned workers.
* **Improved node formatting:** Escaped percent signs are handled correctly in node formatting.
* **CLI error handling:** Command-line status and configuration errors are handled more consistently.
* **Improved preload option handling:** Preload options are passed only to commands that support them.
* **Modernized upgrade utility:** The deprecated ``codecs.open`` usage has been replaced in the settings upgrade command.
* **Improved parser behavior:** ``MultiParser`` honors its configured ``Node`` class.

Utilities and Performance
=========================

Celery 5.7 includes targeted performance and memory-management improvements.

* **Reduced task publishing overhead:** Extension entry-point discovery is cached, reducing repeated discovery overhead in ``apply_async()``.
* **Bounded worker-state memory:** ``State._tasks_to_resolve`` is bounded to prevent unbounded memory growth.
* **Improved buffer accounting:** ``BufferMap`` correctly handles its maximum buffer size and total size accounting.
* **Improved regeneration utilities:** Regeneration iteration and source replacement behave more reliably when iterators are consumed elsewhere.
* **Improved file descriptor iteration:** Safe file descriptor iteration handles attribute errors after connection loss.
* **Improved method lookup:** ``firstmethod()`` no longer silently swallows ``AttributeError``.
* **Improved ``LimitedSet`` behavior:** Re-added and copied items receive correct timestamps and sequence numbers, preventing unexpected expiration or eviction.
* **Improved result-set handling:** Result sets can accept additional results after becoming ready.

Dependencies and Packaging
==========================

Celery 5.7 updates several runtime and development dependencies.

* The minimum Billiard dependency is updated to ``4.3.1``.
* Kombu is updated to the 5.7 release series.
* Redis dependency version constraints have been removed.
* The Brotli extra now uses ``brotlicffi`` instead of ``brotlipy``, with dependency updates for PyPy and CPython.
* Several optional backend dependencies and development tools have been updated.
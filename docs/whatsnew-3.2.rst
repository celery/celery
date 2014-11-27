.. _whatsnew-3.2:

===========================================
 What's new in Celery 3.2 (TBA)
===========================================
:Author: Ask Solem (ask at celeryproject.org)

.. sidebar:: Change history

    What's new documents describe the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible and reliable distributed system to
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

This version is officially supported on CPython 2.6, 2.7 and 3.3,
and also supported on PyPy.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======


.. _v320-important:

Important Notes
===============

Dropped support for Python 2.6
------------------------------

Celery now requires Python 2.7 or later.

JSON is now the default serializer
----------------------------------

Using one logfile per process by default
----------------------------------------

The Task base class no longer automatically register tasks
----------------------------------------------------------

The metaclass has been removed blah blah


Arguments now verified when calling a task
------------------------------------------


.. _v320-news:

News
====

New Task Message Protocol
=========================


``TaskProducer`` replaced by ``app.amqp.create_task_message`` and
``app.amqp.send_task_message``.

- Worker stores results for internal errors like ``ContentDisallowed``, and
  exceptions occurring outside of the task function.


Canvas Refactor
===============

Riak Result Backend
===================

Contributed by Gilles Dartiguelongue, Alman One and NoKriK.

Bla bla

- blah blah


Event Batching
==============

Events are now buffered in the worker and sent as a list


Task.replace
============
 Task.replace changed, removes Task.replace_in_chord.

    The two methods had almost the same functionality, but the old Task.replace
    would force the new task to inherit the callbacks/errbacks of the existing
    task.

    If you replace a node in a tree, then you would not expect the new node to
    inherit the children of the old node, so this seems like unexpected
    behavior.

    So self.replace(sig) now works for any task, in addition sig can now
    be a group.

    Groups are automatically converted to a chord, where the callback
    will "accumulate" the results of the group tasks.

    A new builtin task (`celery.accumulate` was added for this purpose)

    Closes #81


Optimized Beat implementation
=============================

In Other News
-------------

- **Requirements**:

    - Now depends on :ref:`Kombu 3.1 <kombu:version-3.1.0>`.

    - Now depends on :mod:`billiard` version 3.4.

    - No longer depends on ``anyjson`` :sadface:

- **Programs**: ``%n`` format for :program:`celery multi` is now synonym with
  ``%N`` to be consistent with :program:`celery worker`.

- **Programs**: celery inspect/control now supports --json argument

- **Programs**: :program:`celery logtool`: Utility for filtering and parsing celery worker logfiles

- **Worker**: Gossip now sets ``x-message-ttl`` for event queue to heartbeat_interval s.
  (Iss ue #2005).

- **App**: New signals

    - :data:`app.on_configure <@on_configure>`
    - :data:`app.on_after_configure <@on_after_configure>`
    - :data:`app.on_after_finalize <@on_after_finalize>`

- **Canvas**: ``chunks``/``map``/``starmap`` are now routed based on the target task.

- Apps can now define how tasks are named (:meth:`@gen_task_name`).

    Contributed by Dmitry Malinovsky

- Module ``celery.worker.job`` renamed to :mod:`celery.worker.request`.

- Beat: ``Scheduler.Publisher``/``.publisher`` renamed to
  ``.Producer``/``.producer``.


.. _v320-removals:

Scheduled Removals
==================

- The module ``celery.task.trace`` has been removed as scheduled for this
  version.

- Magic keyword arguments no longer supported.

.. _v320-deprecations:

Deprecations
============

See the :ref:`deprecation-timeline`.

.. _v320-fixes:

Fixes
=====

.. _v320-internal:

Internal changes
================

- Module ``celery.worker.job`` has been renamed to :mod:`celery.worker.request`.

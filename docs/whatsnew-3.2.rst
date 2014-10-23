.. _whatsnew-3.1:

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


.. _v320-news:

News
====

Item 1
------

Bla bla

- blah blah

In Other News
-------------

- Now depends on :ref:`Kombu 3.1 <kombu:version-3.1.0>`.

- Now depends on :mod:`billiard` version 3.4.


.. _v320-removals:

Scheduled Removals
==================

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

.. _whatsnew-5.0:

=======================================
 What's new in Celery 5.0 (singularity)
=======================================
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

This version is officially supported on CPython 3.6, 3.7 & 3.8
and is also supported on PyPy3.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

The 5.0.0 release is a new major release for Celery.

Starting from now users should expect more frequent releases of major versions
as we move fast and break things to bring you even better experience.

Releases in the 5.x series are codenamed after songs of `Jon Hopkins <https://en.wikipedia.org/wiki/Jon_Hopkins>`_.
This release has been codenamed `Singularity <https://www.youtube.com/watch?v=lkvnpHFajt0>`_.

This version drops support for Python 2.7.x which has reached EOL
in January 1st, 2020.
This allows us, the maintainers to focus on innovating without worrying
for backwards compatibility.

From now on we only support Python 3.6 and above.
We will maintain compatibility with Python 3.6 until it's
EOL in December, 2021.
We may choose to extend our support if a PyPy version for 3.7 will not become
available by then but we don't guarantee we will.

*— Omer Katz*

Long Term Support Policy
------------------------

As we'd like to provide some time for you to transition,
we're designating Celery 4.x an LTS release.
Celery 4.x will be supported until the 1st of August, 2021.

We will accept and apply patches for bug fixes and security issues.
However, no new features will be merged for that version.

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

Please read the important notes below as there are several breaking changes.

.. _v500-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python Versions are:

- CPython 3.6
- CPython 3.7
- CPython 3.8
- PyPy3.6 7.2 (``pypy3``)

Dropped support for Python 2.7 & 3.5
------------------------------------

Celery now requires Python 3.6 and above.

Python 2.7 has reached EOL in January 2020.
In order to focus our efforts we have dropped support for Python 2.7 in
this version.

In addition Python 3.5 does not provide us with the features we need to move
forward towards Celery 6.x.
Therefore, we are also dropping support for Python 3.5.

If you still require to run Celery using Python 2.7 or Python 3.5
you can still use Celery 4.x.
However we encourage you to upgrade to a supported Python version since
no further security patches will be applied for Python 2.7 and as mentioned
Python 3.5 is not supported for practical reasons.

Kombu
-----

Starting from this release, the minimum required version is Kombu 5.0.0.

Billiard
--------

Starting from this release, the minimum required version is Billiard 3.6.3.

Eventlet Workers Pool
---------------------

Due to `eventlet/eventlet#526 <https://github.com/eventlet/eventlet/issues/526>`_
the minimum required version is eventlet 0.26.1.

Gevent Workers Pool
-------------------

Starting from this release, the minimum required version is gevent 1.0.0.

Couchbase Result Backend
------------------------

The Couchbase result backend now uses the V3 Couchbase SDK.

As a result, we no longer support Couchbase Server 5.x.

Also, starting from this release, the minimum required version
for the database client is couchbase 3.0.0.

To verify that your Couchbase Server is compatible with the V3 SDK,
please refer to their `documentation <https://docs.couchbase.com/python-sdk/3.0/project-docs/compatibility.html>`_.

.. _v500-news:

News
====

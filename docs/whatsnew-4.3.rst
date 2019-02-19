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
Canvas and a number of critical fixes for hanging workers.

Celery 4.3 is the first release to support Python 3.7.

We hope that 4.3 will be the last release to support Python 2.7 as we now
begin to work on Celery 5, the next generation of our task execution platform.

However, if Celery 5 will be delayed for any reason we may release
another 4.x minor version which will still support Python 2.7.

We have also focused on reducing contribution friction.

Thanks to Josue Balandrano Coronel, one of our core contributors, we now have an
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

Important New Kombu Features
----------------------------

Celery 4.3 now depends on Kombu 4.3 and above.

Compression
+++++++++++

Kombu 4.3 includes a few new optional compression methods:

- LZMA (available from stdlib if using Python 3 or from a backported package)
- Brotli (available if you install either the brotli or the brotlipy package)
- ZStandard (available if you install the zstandard package)

Unfortunately our current protocol generates huge payloads for complex canvases.

Until we migrate to our 3rd revision of the Celery protocol in Celery 5
which will resolve this issue, please use one of the new compression methods
as a workaround.

See :ref:`calling-compression` for details.

.. _v430-news:

News
====

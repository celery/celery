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

Artem Vasilyev <artem.v.vasilyev@gmail.com>
Ash Berlin-Taylor <ash_github@firemirror.com>
Asif Saif Uddin (Auvi) <auvipy@gmail.com>
Asif Saif Uddin <auvipy@gmail.com>
Christian Clauss <cclauss@me.com>
Germain Chazot <g.chazot@gmail.com>
Harry Moreno <morenoh149@gmail.com>
kevinbai <kevinbai.cn@gmail.com>
Martin Paulus <mpaulus@lequest.com>
Matus Valo <matusvalo@gmail.com>
Matus Valo <matusvalo@users.noreply.github.com>
maybe-sybr <58414429+maybe-sybr@users.noreply.github.com>
Omer Katz <omer.drow@gmail.com>
Patrick Cloke <clokep@users.noreply.github.com>
qiaocc <jasonqiao36@gmail.com>
Thomas Grainger <tagrain@gmail.com>
Weiliang Li <to.be.impressive@gmail.com>

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

You should mainly verify that any of the breaking changes in the CLI
do not affect you. Please refer to :ref:`New Command Line Interface <new_command_line_interface>` for details.

Step 4: Migrate your code to Python 3
-------------------------------------

Celery 5.0 supports only Python 3. Therefore, you must ensure your code is
compatible with Python 3.

If you haven't ported your code to Python 3, you must do so before upgrading.

You can use tools like `2to3 <https://docs.python.org/3.8/library/2to3.html>`_
and `pyupgrade <https://github.com/asottile/pyupgrade>`_ to assist you with
this effort.

After the migration is done, run your test suite with Celery 4 to ensure
nothing has been broken.

Step 5: Upgrade to Celery 5.0
-----------------------------

At this point you can upgrade your workers and clients with the new version.

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

In addition, Python 3.5 has reached EOL in September 2020.
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

Riak Result Backend
-------------------

The Riak result backend has been removed as the database is no longer maintained.

The Python client only supports Python 3.6 and below which prevents us from
supporting it and it is also unmaintained.

If you are still using Riak, refrain from upgrading to Celery 5.0 while you
migrate your application to a different database.

We apologize for the lack of notice in advance but we feel that the chance
you'll be affected by this breaking change is minimal which is why we
did it.

AMQP Result Backend
-------------------

The AMQP result backend has been removed as it was deprecated in version 4.0.

Removed Deprecated Modules
--------------------------

The `celery.utils.encoding` and the `celery.task` modules has been deprecated
in version 4.0 and therefore are removed in 5.0.

If you were using the `celery.utils.encoding` module before,
you should import `kombu.utils.encoding` instead.

If you were using the `celery.task` module before, you should import directly
from the `celery` module instead.

.. _new_command_line_interface:

New Command Line Interface
--------------------------

The command line interface has been revamped using Click.
As a result a few breaking changes has been introduced:

- Postfix global options like `celery worker --app path.to.app` or `celery worker --workdir /path/to/workdir` are no longer supported.
  You should specify them as part of the global options of the main celery command.
- :program:`celery amqp` and :program:`celery shell` require the `repl`
  sub command to start a shell. You can now also invoke specific commands
  without a shell. Type `celery amqp --help` or `celery shell --help` for details.
- The API for adding user options has changed.
  Refer to the :ref:`documentation <extending-command-options>` for details.

Click provides shell completion `out of the box <https://click.palletsprojects.com/en/7.x/bashcomplete/>`_.
This functionality replaces our previous bash completion script and adds
completion support for the zsh and fish shells.

The bash completion script was exported to `extras/celery.bash <https://github.com/celery/celery/blob/master/extra/bash-completion/celery.bash>`_
for the packager's convenience.

Pytest Integration
------------------

Starting from Celery 5.0, the pytest plugin is no longer enabled by default.

Please refer to the :ref:`documentation <pytest_plugin>` for instructions.

Ordered Group Results for the Redis Result Backend
--------------------------------------------------

Previously group results were not ordered by their invocation order.
Celery 4.4.7 introduced an opt-in feature to make them ordered.

It is now an opt-out behavior.

If you were previously using the Redis result backend, you might need to
out-out of this behavior.

Please refer to the :ref:`documentation <redis-group-result-ordering>`
for instructions on how to disable this feature.

.. _v500-news:

News
====

Retry Policy for the Redis Result Backend
-----------------------------------------

The retry policy for the Redis result backend is now exposed through
the result backend transport options.

Please refer to the :ref:`documentation <redis-result-backend-timeout>` for details.

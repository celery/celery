.. _changelog-2.5:

===============================
 Change history for Celery 2.5
===============================

This document contains change notes for bugfix releases in the 2.5.x series,
please see :ref:`whatsnew-2.5` for an overview of what's
new in Celery 2.5.

If you're looking for versions prior to 2.5 you should visit our
:ref:`history` of releases.

.. contents::
    :local:

.. _version-2.5.5:

2.5.5
=====
:release-date: 2012-06-06 04:00 p.m. BST
:release-by: Ask Solem

This is a dummy release performed for the following goals:

- Protect against force upgrading to Kombu 2.2.0
- Version parity with :pypi:`django-celery`

.. _version-2.5.3:

2.5.3
=====
:release-date: 2012-04-16 07:00 p.m. BST
:release-by: Ask Solem

* A bug causes messages to be sent with UTC time-stamps even though
  :setting:`CELERY_ENABLE_UTC` wasn't enabled (Issue #636).

* ``celerybeat``: No longer crashes if an entry's args is set to None
  (Issue #657).

* Auto-reload didn't work if a module's ``__file__`` attribute
  was set to the modules ``.pyc`` file.  (Issue #647).

* Fixes early 2.5 compatibility where ``__package__`` doesn't exist
  (Issue #638).

.. _version-2.5.2:

2.5.2
=====
:release-date: 2012-04-13 04:30 p.m. GMT
:release-by: Ask Solem

.. _v252-news:

News
----

- Now depends on Kombu 2.1.5.

- Django documentation has been moved to the main Celery docs.

    See :ref:`django`.

- New :signal:`celeryd_init` signal can be used to configure workers
  by hostname.

- Signal.connect can now be used as a decorator.

    Example:

    .. code-block:: python

        from celery.signals import task_sent

        @task_sent.connect
        def on_task_sent(**kwargs):
            print('sent task: %r' % (kwargs,))

- Invalid task messages are now rejected instead of acked.

    This means that they will be moved to the dead-letter queue
    introduced in the latest RabbitMQ version (but must be enabled
    manually, consult the RabbitMQ documentation).

- Internal logging calls has been cleaned up to work
  better with tools like Sentry.

    Contributed by David Cramer.

- New method ``subtask.clone()`` can be used to clone an existing
  subtask with augmented arguments/options.

    Example:

    .. code-block:: pycon

        >>> s = add.subtask((5,))
        >>> new = s.clone(args=(10,), countdown=5})
        >>> new.args
        (10, 5)

        >>> new.options
        {'countdown': 5}

- Chord callbacks are now triggered in eager mode.

.. _v252-fixes:

Fixes
-----

- Programs now verifies that the pidfile is actually written correctly
  (Issue #641).

    Hopefully this will crash the worker immediately if the system
    is out of space to store the complete pidfile.

    In addition, we now verify that existing pidfiles contain
    a new line so that a partially written pidfile is detected as broken,
    as before doing:

    .. code-block:: console

        $ echo -n "1" > celeryd.pid

    would cause the worker to think that an existing instance was already
    running (init has pid 1 after all).

- Fixed 2.5 compatibility issue with use of print_exception.

    Fix contributed by Martin Melin.

- Fixed 2.5 compatibility issue with imports.

    Fix contributed by Iurii Kriachko.

- All programs now fix up ``__package__`` when called as main.

    This fixes compatibility with Python 2.5.

    Fix contributed by Martin Melin.

- [celery control|inspect] can now be configured on the command-line.

    Like with the worker it is now possible to configure Celery settings
    on the command-line for celery control|inspect

    .. code-block:: console

        $ celery inspect -- broker.pool_limit=30

- Version dependency for :pypi:`python-dateutil` fixed to be strict.

    Fix contributed by Thomas Meson.

- ``Task.__call__`` is now optimized away in the task tracer
  rather than when the task class is created.

    This fixes a bug where a custom __call__  may mysteriously disappear.

- Auto-reload's ``inotify`` support has been improved.

    Contributed by Mher Movsisyan.

- The Django broker documentation has been improved.

- Removed confusing warning at top of routing user guide.

.. _version-2.5.1:

2.5.1
=====
:release-date: 2012-03-01 01:00 p.m. GMT
:release-by: Ask Solem

.. _v251-fixes:

Fixes
-----

* Eventlet/Gevent: A small typo caused the worker to hang when eventlet/gevent
  was used, this was because the environment wasn't monkey patched
  early enough.

* Eventlet/Gevent: Another small typo caused the mediator to be started
  with eventlet/gevent, which would make the worker sometimes hang at shutdown.

* :mod:`multiprocessing`: Fixed an error occurring if the pool was stopped
  before it was properly started.

* Proxy objects now redirects ``__doc__`` and ``__name__`` so ``help(obj)``
  works.

* Internal timer (timer2) now logs exceptions instead of swallowing them
  (Issue #626).

* celery shell: can now be started with
  :option:`--eventlet <celery shell --eventlet>` or
  :option:`--gevent <celery shell --gevent>` options to apply their
  monkey patches.

.. _version-2.5.0:

2.5.0
=====
:release-date: 2012-02-24 04:00 p.m. GMT
:release-by: Ask Solem

See :ref:`whatsnew-2.5`.

Since the changelog has gained considerable size, we decided to
do things differently this time: by having separate "what's new"
documents for major version changes.

Bugfix releases will still be found in the changelog.


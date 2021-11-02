.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the & 5.2.x series, please see :ref:`whatsnew-5.2` for
an overview of what's new in Celery 5.2.

.. _version-5.2.0rc1:

5.2.0rc1
=======
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

.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the & 5.2.x series, please see :ref:`whatsnew-5.2` for
an overview of what's new in Celery 5.2.


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

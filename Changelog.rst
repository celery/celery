.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the 5.0.x series, please see :ref:`whatsnew-5.0` for
an overview of what's new in Celery 5.0.


5.0.0
=====
:release-date: N/A
:release-by: Omer Katz


5.0.0rc3
========
:release-date: 2020-09-07 4.00 P.M UTC+3:00
:release-by: Omer Katz

- More cleanups of leftover Python 2 support. (#6338)

5.0.0rc2
========
:release-date: 2020-09-01 6.30 P.M UTC+3:00
:release-by: Omer Katz

- Bump minimum required eventlet version to 0.26.1.
- Update Couchbase Result backend to use SDK V3.
- Restore monkeypatching when gevent or eventlet are used.

5.0.0rc1
========
:release-date: 2020-08-24 9.00 P.M UTC+3:00
:release-by: Omer Katz

- Allow to opt out of ordered group results when using the Redis result backend (#6290).
- **Breaking Chnage** Remove the deprecated celery.utils.encoding module.

5.0.0b1
=======
:release-date: 2020-08-19 8.30 P.M UTC+3:00
:release-by: Omer Katz

- **Breaking Chnage** Drop support for the Riak result backend (#5686).
- **Breaking Chnage** pytest plugin is no longer enabled by default. (#6288)
  Install pytest-celery to enable it.
- **Breaking Chnage** Brand new CLI based on Click (#5718).

5.0.0a2
=======
:release-date: 2020-08-05 7.15 P.M UTC+3:00
:release-by: Omer Katz

- Bump Kombu version to 5.0 (#5686).

5.0.0a1
=======
:release-date: 2020-08-02 9.30 P.M UTC+3:00
:release-by: Omer Katz

- Removed most of the compatibility code that supports Python 2 (#5686).
- Modernized code to work on Python 3.6 and above (#5686).

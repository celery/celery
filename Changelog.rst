.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the 5.0.x series, please see :ref:`whatsnew-5.0` for
an overview of what's new in Celery 5.0.

.. _version-5.0.5:

5.0.5
=====
:release-date: 2020-12-16 5.35 P.M UTC+2:00
:release-by: Omer Katz

- Ensure keys are strings when deleting results from S3 (#6537).
- Fix a regression breaking `celery --help` and `celery events` (#6543).

.. _version-5.0.4:

5.0.4
=====
:release-date: 2020-12-08 2.40 P.M UTC+2:00
:release-by: Omer Katz

-  DummyClient of cache+memory:// backend now shares state between threads (#6524).

   This fixes a problem when using our pytest integration with the in memory
   result backend.
   Because the state wasn't shared between threads, #6416 results in test suites
   hanging on `result.get()`.

.. _version-5.0.3:

5.0.3
=====
:release-date: 2020-12-03 6.30 P.M UTC+2:00
:release-by: Omer Katz

- Make `--workdir` eager for early handling (#6457).
- When using the MongoDB backend, don't cleanup if result_expires is 0 or None (#6462).
- Fix passing queues into purge command (#6469).
- Restore `app.start()` and `app.worker_main()` (#6481).
- Detaching no longer creates an extra log file (#6426).
- Result backend instances are now thread local to ensure thread safety (#6416).
- Don't upgrade click to 8.x since click-repl doesn't support it yet.
- Restore preload options (#6516).

.. _version-5.0.2:

5.0.2
=====
:release-date: 2020-11-02 8.00 P.M UTC+2:00
:release-by: Omer Katz

- Fix _autodiscover_tasks_from_fixups (#6424).
- Flush worker prints, notably the banner (#6432).
- **Breaking Change**: Remove `ha_policy` from queue definition. (#6440)

    This argument has no effect since RabbitMQ 3.0.
    Therefore, We feel comfortable dropping it in a patch release.

- Python 3.9 support (#6418).
- **Regression**: When using the prefork pool, pick the fair scheduling strategy by default (#6447).
- Preserve callbacks when replacing a task with a chain (#6189).
- Fix max_retries override on `self.retry()` (#6436).
- Raise proper error when replacing with an empty chain (#6452)

.. _version-5.0.1:

5.0.1
=====
:release-date: 2020-10-18 1.00 P.M UTC+3:00
:release-by: Omer Katz

- Specify UTF-8 as the encoding for log files (#6357).
- Custom headers now propagate when using the protocol 1 hybrid messages (#6374).
- Retry creating the database schema for the database results backend
  in case of a race condition (#6298).
- When using the Redis results backend, awaiting for a chord no longer hangs
  when setting :setting:`result_expires` to 0 (#6373).
- When a user tries to specify the app as an option for the subcommand,
  a custom error message is displayed (#6363).
- Fix the `--without-gossip`, `--without-mingle`, and `--without-heartbeat`
  options which now work as expected. (#6365)
- Provide a clearer error message when the application cannot be loaded.
- Avoid printing deprecation warnings for settings when they are loaded from
  Django settings (#6385).
- Allow lowercase log levels for the `--loglevel` option (#6388).
- Detaching now works as expected (#6401).
- Restore broadcasting messages from `celery control` (#6400).
- Pass back real result for single task chains (#6411).
- Ensure group tasks a deeply serialized (#6342).
- Fix chord element counting (#6354).
- Restore the `celery shell` command (#6421).

.. _version-5.0.0:

5.0.0
=====
:release-date: 2020-09-24 6.00 P.M UTC+3:00
:release-by: Omer Katz

- **Breaking Change** Remove AMQP result backend (#6360).
- Warn when deprecated settings are used (#6353).
- Expose retry_policy for Redis result backend (#6330).
- Prepare Celery to support the yet to be released Python 3.9 (#6328).

5.0.0rc3
========
:release-date: 2020-09-07 4.00 P.M UTC+3:00
:release-by: Omer Katz

- More cleanups of leftover Python 2 support (#6338).

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
- **Breaking Change** Remove the deprecated celery.utils.encoding module.

5.0.0b1
=======
:release-date: 2020-08-19 8.30 P.M UTC+3:00
:release-by: Omer Katz

- **Breaking Change** Drop support for the Riak result backend (#5686).
- **Breaking Change** pytest plugin is no longer enabled by default (#6288).
  Install pytest-celery to enable it.
- **Breaking Change** Brand new CLI based on Click (#5718).

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

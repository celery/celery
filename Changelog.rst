.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the & 5.1.x series, please see :ref:`whatsnew-5.1` for
an overview of what's new in Celery 5.1.

.. version-5.1.2:

5.1.2
=====
:release-date: 2021-06-28 16.15 P.M UTC+3:00
:release-by: Omer Katz

- When chords fail, correctly call errbacks. (#6814)

    We had a special case for calling errbacks when a chord failed which
    assumed they were old style. This change ensures that we call the proper
    errback dispatch method which understands new and old style errbacks,
    and adds test to confirm that things behave as one might expect now.
- Avoid using the ``Event.isSet()`` deprecated alias. (#6824)
- Reintroduce sys.argv default behaviour for ``Celery.start()``. (#6825)

.. version-5.1.1:

5.1.1
=====
:release-date: 2021-06-17 16.10 P.M UTC+3:00
:release-by: Omer Katz

- Fix ``--pool=threads`` support in command line options parsing. (#6787)
- Fix ``LoggingProxy.write()`` return type. (#6791)
- Couchdb key is now always coerced into a string. (#6781)
- grp is no longer imported unconditionally. (#6804)
    This fixes a regression in 5.1.0 when running Celery in non-unix systems.
- Ensure regen utility class gets marked as done when concertised. (#6789)
- Preserve call/errbacks of replaced tasks. (#6770)
- Use single-lookahead for regen consumption. (#6799)
- Revoked tasks are no longer incorrectly marked as retried. (#6812, #6816)

.. version-5.1.0:

5.1.0
=====
:release-date: 2021-05-23 19.20 P.M UTC+3:00
:release-by: Omer Katz

- ``celery -A app events -c camera`` now works as expected. (#6774)
- Bump minimum required Kombu version to 5.1.0.

.. _version-5.1.0rc1:

5.1.0rc1
========
:release-date: 2021-05-02 16.06 P.M UTC+3:00
:release-by: Omer Katz

- Celery Mailbox accept and serializer parameters are initialized from configuration. (#6757)
- Error propagation and errback calling for group-like signatures now works as expected. (#6746)
- Fix sanitization of passwords in sentinel URIs. (#6765)
- Add LOG_RECEIVED to customize logging. (#6758)

.. _version-5.1.0b2:

5.1.0b2
=======
:release-date: 2021-05-02 16.06 P.M UTC+3:00
:release-by: Omer Katz

- Fix the behavior of our json serialization which regressed in 5.0. (#6561)
- Add support for SQLAlchemy 1.4. (#6709)
- Safeguard against schedule entry without kwargs. (#6619)
- ``task.apply_async(ignore_result=True)`` now avoids persisting the results. (#6713)
- Update systemd tmpfiles path. (#6688)
- Ensure AMQPContext exposes an app attribute. (#6741)
- Inspect commands accept arguments again (#6710).
- Chord counting of group children is now accurate. (#6733)
- Add a setting :setting:`worker_cancel_long_running_tasks_on_connection_loss`
  to terminate tasks with late acknowledgement on connection loss. (#6654)
- The ``task-revoked`` event and the ``task_revoked`` signal are not duplicated
  when ``Request.on_failure`` is called. (#6654)
- Restore pickling support for ``Retry``. (#6748)
- Add support in the redis result backend for authenticating with a username. (#6750)
- The :setting:`worker_pool` setting is now respected correctly. (#6711)

.. _version-5.1.0b1:

5.1.0b1
=======
:release-date: 2021-04-02 10.25 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Add sentinel_kwargs to Redis Sentinel docs.
- Depend on the maintained python-consul2 library. (#6544).
- Use result_chord_join_timeout instead of hardcoded default value.
- Upgrade AzureBlockBlob storage backend to use Azure blob storage library v12 (#6580).
- Improved integration tests.
- pass_context for handle_preload_options decorator (#6583).
- Makes regen less greedy (#6589).
- Pytest worker shutdown timeout (#6588).
- Exit celery with non zero exit value if failing (#6602).
- Raise BackendStoreError when set value is too large for Redis.
- Trace task optimizations are now set via Celery app instance.
- Make trace_task_ret and fast_trace_task public.
- reset_worker_optimizations and create_request_cls has now app as optional parameter.
- Small refactor in exception handling of on_failure (#6633).
- Fix for issue #5030 "Celery Result backend on Windows OS".
- Add store_eager_result setting so eager tasks can store result on the result backend (#6614).
- Allow heartbeats to be sent in tests (#6632).
- Fixed default visibility timeout note in sqs documentation.
- Support Redis Sentinel with SSL.
- Simulate more exhaustive delivery info in apply().
- Start chord header tasks as soon as possible (#6576).
- Forward shadow option for retried tasks (#6655).
- --quiet flag now actually makes celery avoid producing logs (#6599).
- Update platforms.py "superuser privileges" check (#6600).
- Remove unused property `autoregister` from the Task class (#6624).
- fnmatch.translate() already translates globs for us. (#6668).
- Upgrade some syntax to Python 3.6+.
- Add `azureblockblob_base_path` config (#6669).
- Fix checking expiration of X.509 certificates (#6678).
- Drop the lzma extra.
- Fix JSON decoding errors when using MongoDB as backend (#6675).
- Allow configuration of RedisBackend's health_check_interval (#6666).
- Safeguard against schedule entry without kwargs (#6619).
- Docs only - SQS broker - add STS support (#6693) through kombu.
- Drop fun_accepts_kwargs backport.
- Tasks can now have required kwargs at any order (#6699).
- Min py-amqp 5.0.6.
- min billiard is now 3.6.4.0.
- Minimum kombu now is5.1.0b1.
- Numerous docs fixes.
- Moved CI to github action.
- Updated deployment scripts.
- Updated docker.
- Initial support of python 3.9 added.

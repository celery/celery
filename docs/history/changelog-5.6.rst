.. _changelog-5.6:

================
 Change history
================

This document contains change notes for bugfix & new features
in the main branch & 5.6.x series, please see :ref:`whatsnew-5.6` for
an overview of what's new in Celery 5.6.

.. _version-5.6.0rc1:

5.6.0rc1
========

:release-date: 2025-11-02
:release-by: Tomer Nosrati

Celery v5.6.0 Release Candidate 1 is now available for testing.
Please help us test this version and report any issues.

What's Changed
~~~~~~~~~~~~~~

- Add support for Django Connection pool (#9953)
- Pin tblib to ==3.1.0 (#9967)
- fix(worker): continue to attempt to bind other queues after a native delayed delivery binding failure has occurred (#9959)
- Handle UnpicklingError in persistent scheduler initialization (#9952)
- Bug Fix: Nested Chords Fail When Using django-celery-results with a Redis Backend (#9950)
- Add support pymongo  4.12 (#9665)
- Make tests compatible with pymongo >= 4.14 (#9968)
- tblib updated from 3.1.0 to 3.2.0 (#9970)
- Fix remaining function typing and docstring (#9971)
- Fix regex pattern in version parsing and remove duplicate entry in __all__ (#9978)
- Bump Kombu to v5.6.0 and removed <5.7 limit on kombu (#9981)
- Prepare for (pre) release: v5.6.0rc1 (#9982)

.. _version-5.6.0b2:

5.6.0b2
=======

:release-date: 2025-10-20
:release-by: Tomer Nosrati

Celery v5.6.0 Beta 2 is now available for testing.
Please help us test this version and report any issues.

What's Changed
~~~~~~~~~~~~~~

- GitHub Actions: Test on Python 3.14 release candidate 2 (#9891)
- Update pypy to python 3.11 (#9896)
- Feature: Add support credential_provider to Redis Backend (#9879)
- Celery.timezone: try tzlocal.get_localzone() before using LocalTimezone (#9862)
- Run integration tests on Python 3.14 (#9903)
- Fix arithmetic overflow for MSSQL result backend (#9904)
- Add documentation for task_id param for apply_async function (#9906)
- Support redis client name (#9900)
- Bump Kombu to v5.6.0rc1 (#9918)
- Fix broker connection retry attempt counter in the error log (#9911)
- fix: restrict disable-prefetch feature to Redis brokers only (#9919)
- fix(): preserve group order in replaced signature (#9910)
- Remove Python 3.8 from CI workflow (#9930)
- Update default Python versions in integration tests (#9931)
- Update tox.ini to remove Python 3.8 (#9932)
- Remove Python 3.8 from Dockerfile (#9933)
- Update Python version requirement to 3.9 (#9935)
- Update pypy version from 3.10 to 3.11 in Dockerfile (#9934)
- Flake8 fixes (#9955)
- Remove test-pypy3.txt from Dockerfile dependencies (#9939)
- Remove backports.zoneinfo for Python 3.9 compatibility (#9956)
- Update pytest-cov version for Python compatibility (#9957)
- Update pytest-rerunfailures and pre-commit versions (#9958)
- Prepare for (pre) release: v5.6.0b2 (#9938)

.. _version-5.6.0b1:

5.6.0b1
=======

:release-date: 2025-09-15
:release-by: Tomer Nosrati

Celery v5.6.0 Beta 1 is now available for testing.
Please help us test this version and report any issues.

What's Changed
~~~~~~~~~~~~~~

- docs: mention of json serializer recursive reference message size blowup (#5000) (#9743)
- docs: typo in canvas.rst (#9744)
- Makes _on_retry return a float as required to be used as errback on retry_over_time (#9741)
- Update canvas.rst doc calculation order for callback (#9758)
- Updated Blacksmith logo (#9763)
- Made the Sponsors logos link to their website (#9764)
- add missing cloudamqp logo (#9767)
- Improve sponsor visibility (#9768)
- fix: (#9773) task_id must not be empty with chain as body of a chord (#9774)
- Update setup.py to fix deprecation warning (#9771)
- Adds integration test for chord_unlock bug when routed to quorum/topic queue (#9766)
- Add xfail test for default queue/exchange fallback ignoring task_default_* settings (#9765)
- Add xfail test for RabbitMQ quorum queue global QoS race condition (#9770)
- fix: (#8786) time out when chord header fails with group body (#9788)
- Fix #9738 : Add root_id and parent_id to .apply() (#9784)
- Replace DelayedDelivery connection creation to use context manger (#9793)
- Fix #9794: Pydantic integration fails with __future__.annotations. (#9795)
- add go and rust implementation in docs (#9800)
- Fix memory leak in exception handling (Issue #8882) (#9799)
- Fix handlers docs (Issue #9787) (#9804)
- Remove importlib_metadata leftovers (#9791)
- Update timeout minutes for smoke tests CI (#9807)
- Revert "Remove dependency on `pycurl`" (#9620)
- Add Blacksmith Docker layer caching to all Docker builds (#9840)
- Bump Kombu to v5.6.0b1 (#9839)
- Disable pytest-xdist for smoke tests and increase retries (CI ONLY) (#9842)
- Fix Python 3.13 compatibility in events dumper (#9826)
- Dockerfile Build Optimizations (#9733)
- Migrated from useblacksmith/build-push-action@v1 to useblacksmith/setup-docker-builder@v1 in the CI (#9846)
- Remove incorrect example (#9854)
- Revert "Use Django DB max age connection setting" (#9824)
- Fix pending_result memory leak (#9806)
- Update python-package.yml (#9856)
- Bump Kombu to v5.6.0b2 (#9858)
- Refactor integration and smoke tests CI (#9855)
- Fix `AsyncResult.forget()` with couchdb backend method raises `TypeError: a bytes-like object is required, not 'str'` (#9865)
- Improve Docs for SQS Authentication (#9868)
- Added `.github/copilot-instructions.md` for GitHub Copilot (#9874)
- misc: credit removal (#9877)
- Choose queue type and exchange type when creating missing queues (fix #9671) (#9815)
- fix: prevent celery from hanging due to spawned greenlet errors in greenlet drainers (#9371)
- Feature/disable prefetch fixes (#9863)
- Add worker_eta_task_limit configuration to manage ETA task memory usage (#9853)
- Update runner version in Docker workflow (#9884)
- Prepare for (pre) release: v5.6.0b1 (#9890)

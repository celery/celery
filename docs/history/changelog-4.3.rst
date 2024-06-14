.. _changelog-4.3:

================
 Change history
================

This document contains change notes for bugfix releases in
the 4.3.x series, please see :ref:`whatsnew-4.3` for
an overview of what's new in Celery 4.3.

4.3.1
=====

:release-date: 2020-09-10 1:00 P.M UTC+3:00
:release-by: Omer Katz

- Limit vine version to be below 5.0.0.

  Contributed by **Omer Katz**

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

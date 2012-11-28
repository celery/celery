.. _whatsnew-3.1:

===========================================
 What's new in Celery 3.1 (Cipater)
===========================================

.. sidebar:: Change history

    What's new documents describes the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible and reliable distributed system to
process vast amounts of messages, while providing operations with
the tools required to maintain such a system.

It's a task queue with focus on real-time processing, while also
supporting task scheduling.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is backward compatible with previous versions
it's important that you read the following section.

This version is officially supported on CPython 2.6, 2.7, 3.2 and 3.3,
as well as PyPy and Jython.

Highlights
==========

.. topic:: Overview

    - XXX1

        YYY1

    - XXX2

    - XXX3

        YYY3

.. _`website`: http://celeryproject.org/
.. _`django-celery changelog`:
    http://github.com/celery/django-celery/tree/master/Changelog
.. _`django-celery 3.0`: http://pypi.python.org/pypi/django-celery/

.. contents::
    :local:
    :depth: 2

.. _v310-important:

Important Notes
===============

XXX
---

YYY

.. _v310-news:

News
====

XXX
---

YYY

In Other News
-------------

- No longer supports Python 2.5

    From this version Celery requires Python 2.6 or later.

- No longer depends on ``python-dateutil``

    Instead a dependency on :mod:`pytz` has been added, which was already
    recommended in the documentation for accurate timezone support.

    This also means that dependencies are on the same on both Python 2 and
    Python 3, and that the :file:`requirements/default-py3k.txt` file has
    been removed.

- Time limits can now be set by the client for individual tasks (Issue #802).

    You can set both hard and soft time limits using the ``timeout`` and
    ``soft_timeout`` calling options:

    .. code-block:: python

        >>> res = add.apply_async((2, 2), timeout=10, soft_timeout=8)

        >>> res = add.subtask((2, 2), timeout=10, soft_timeout=8)()

        >>> res = add.s(2, 2).set(timeout=10, soft_timeout=8)()

    Contributed by Mher Movsisyan.

- Old command-line programs removed and deprecated

    The goal is that everyone should move the new :program:`celery` umbrella
    command, so with this version we deprecate the old command names,
    and remove commands that are not used in init scripts.

    +-------------------+--------------+-------------------------------------+
    | Program           | New Status   | Replacement                         |
    +===================+==============+=====================================+
    | ``celeryd``       | *DEPRECATED* | :program:`celery worker`            |
    +-------------------+--------------+-------------------------------------+
    | ``celerybeat``    | *DEPRECATED* | :program:`celery beat`              |
    +-------------------+--------------+-------------------------------------+
    | ``celeryd-multi`` | *DEPRECATED* | :program:`celery multi`             |
    +-------------------+--------------+-------------------------------------+
    | ``celeryctl``     | **REMOVED**  | :program:`celery`                   |
    +-------------------+--------------+-------------------------------------+
    | ``celeryev``      | **REMOVED**  | :program:`celery events`            |
    +-------------------+--------------+-------------------------------------+
    | ``camqadm``       | **REMOVED**  | :program:`celery amqp`              |
    +-------------------+--------------+-------------------------------------+

    Please see :program:`celery --help` for help using the umbrella command.

- Celery now support Django out of the box.

    The fixes and improvements applied by the django-celery library is now
    automatically applied by core Celery when it detects that
    the :envvar:`DJANGO_SETTINGS_MODULE` environment setting is set.

    The distribution ships with a new example project using Django
    in :file:`examples/django`:

    http://github.com/celery/celery/tree/master/examples/django

    There are cases where you would want to use django-celery still
    as:

        - Celery does not implement the Django database or cache backends.
        - Celery does not automatically read configuration from Django settings.
        - Celery does not ship with the database-based periodic task
          scheduler.

    If you are using django-celery then it is crucial that you have
    ``djcelery.setup_loader()`` in your settings module, as this
    no longer happens as a side-effect of importing the :mod:`djcelery`
    module.

- The consumer part of the worker has been rewritten to use Bootsteps.

    By writing bootsteps you can now easily extend the consumer part
    of the worker to add additional features, or even message consumers.

    See the :ref:`guide-extending` guide for more information.

- New Bootsteps implementation.

    The bootsteps and namespaces have been refactored for the better,
    sadly this means that bootsteps written for older versions will
    not be compatible with this version.

    Bootsteps were never publicly documented and was considered
    experimental, so chances are no one has ever implemented custom
    bootsteps, but if you did please contact the mailing-list
    and we'll help you port them.

    - Module ``celery.worker.bootsteps`` renamed to :mod:`celery.bootsteps`
    - The name of a bootstep no longer contain the name of the namespace.
    - A bootstep can now be part of multiple namespaces.
    - Namespaces must instantiate individual bootsteps, and
      there's no global registry of bootsteps.



- New result backend with RPC semantics (``rpc``).

    This version of the ``amqp`` result backend is a very good alternative
    to use in classical RPC scenarios, where the process that initiates
    the task is always the process to retrieve the result.

    It uses Kombu to send and retrieve results, and each client
    will create a unique queue for replies to be sent to. Avoiding
    the significant overhead of the original amqp backend which creates
    one queue per task, but it's important to consider that it will
    not be possible to retrieve the result from another process,
    and that results sent using this backend is not persistent and so will
    not survive a broker restart.

    It has only been tested with the AMQP and Redis transports.

- App instances can now add additional command line options
  to the worker and beat programs.

    The :attr:`@Celery.user_options` attribute can be used
    to add additional command-line arguments, and expects
    optparse-style options:

    .. code-block:: python

        from celery import Celery
        from optparse import make_option as Option

        celery = Celery()
        celery.user_options['worker'].add(
            Option('--my-argument'),
        )

    See :ref:`guide-extending` for more information.

- Events are now ordered using logical time.

    Timestamps are not a reliable way to order events in a distributed system,
    for one the floating point value does not have enough precision, but
    also it's impossible to keep physical clocks in sync.

    Celery event messages have included a logical clock value for some time,
    but starting with this version that field is also used to order them
    (if the monitor is using ``celery.events.state``).

    The logical clock is currently implemented using Lamport timestamps,
    which does not have a high degree of accuracy, but should be good
    enough to casually order the events.

- All events now include a ``pid`` field, which is the process id of the
  process that sent the event.

- Events now supports timezones.

    A new ``utcoffset`` field is now sent with every event.  This is a
    signed integer telling the difference from UTC time in hours,
    so e.g. an even sent from the Europe/London timezone in daylight savings
    time will have an offset of 1.

    :class:`@events.Receiver` will automatically convert the timestamps
    to the destination timezone.

- Event heartbeats are now calculated based on the time when the event
  was received by the monitor, and not the time reported by the worker.

    This means that a worker with an out-of-sync clock will no longer
    show as 'Offline' in monitors.

    A warning is now emitted if the difference between the senders
    time and the internal time is greater than 15 seconds, suggesting
    that the clocks are out of sync.

- :program:`celery worker` now supports a ``--detach`` argument to start
  the worker as a daemon in the background.

- :class:`@events.Receiver` now sets a ``local_received`` field for incoming
  events, which is set to the time of when the event was received.

- :class:`@events.Dispatcher` now accepts a ``groups`` argument
  which decides a whitelist of event groups that will be sent.

    The type of an event is a string separated by '-', where the part
    before the first '-' is the group.  Currently there are only
    two groups: ``worker`` and ``task``.

    A dispatcher instantiated as follows:

    .. code-block:: python

        app.events.Dispatcher(connection, groups=['worker'])

    will only send worker related events and silently drop any attempts
    to send events related to any other group.

- Better support for link and link_error tasks for chords.

    Contributed by Steeve Morin.

- There's a now an 'inspect clock' command which will collect the current
  logical clock value from workers.

- `celery inspect stats` now contains the process id of the worker's main
  process.

    Contributed by Mher Movsisyan.

- New remote control command to dump a workers configuration.

    Example:

    .. code-block:: bash

        $ celery inspect conf

    Configuration values will be converted to values supported by JSON
    where possible.

    Contributed by Mher Movisyan.


- Now supports Setuptools extra requirements.

    +-------------+-------------------------+---------------------------+
    | Extension   | Requirement entry       | Type                      |
    +=============+=========================+===========================+
    | Redis       | ``celery[redis]``       | transport, result backend |
    +-------------+-------------------------+---------------------------+
    | MongoDB``   | ``celery[mongodb]``     | transport, result backend |
    +-------------+-------------------------+---------------------------+
    | CouchDB     | ``celery[couchdb]``     | transport                 |
    +-------------+-------------------------+---------------------------+
    | Beanstalk   | ``celery[beanstalk]``   | transport                 |
    +-------------+-------------------------+---------------------------+
    | ZeroMQ      | ``celery[zeromq]``      | transport                 |
    +-------------+-------------------------+---------------------------+
    | Zookeeper   | ``celery[zookeeper]``   | transport                 |
    +-------------+-------------------------+---------------------------+
    | SQLAlchemy  | ``celery[sqlalchemy]``  | transport, result backend |
    +-------------+-------------------------+---------------------------+
    | librabbitmq | ``celery[librabbitmq]`` | transport (C amqp client) |
    +-------------+-------------------------+---------------------------+

    Examples using :program:`pip install`:

    .. code-block:: bash

        pip install celery[redis]
        pip install celery[librabbitmq]

        pip install celery[redis,librabbitmq]

        pip install celery[mongodb]
        pip install celery[couchdb]
        pip install celery[beanstalk]
        pip install celery[zeromq]
        pip install celery[zookeeper]
        pip install celery[sqlalchemy]

- Worker node names now consists of a name and a hostname separated by '@'.

    This change is to more easily identify multiple instances running
    on the same machine.

    If a custom name is not specified then the
    worker will use the name 'celery' in default, resulting in a
    fully qualified node name of 'celery@hostname':

    .. code-block:: bash

        $ celery worker -n example.com
        celery@example.com

    To set the name you must include the @:

    .. code-block:: bash

        $ celery worker -n worker1@example.com
        worker1@example.com

    This also means that the worker will identify itself using the full
    nodename in events and broadcast messages, so where before
    a worker would identify as 'worker1.example.com', it will now
    use 'celery@worker1.example.com'.

    Remember that the ``-n`` argument also supports simple variable
    substitutions, so if the current hostname is *jerry.example.com*
    then ``%h`` will expand into that:

    .. code-block:: bash

        $ celery worker -n worker1@%h
        worker1@jerry.example.com

    The table of substitutions is as follows:

    +---------------+---------------------------------------+
    | Variable      | Substitution                          |
    +===============+=======================================+
    | ``%h``        | Full hostname (including domain name) |
    +---------------+---------------------------------------+
    | ``%d``        | Domain name only                      |
    +---------------+---------------------------------------+
    | ``%n``        | Hostname only (without domain name)   |
    +---------------+---------------------------------------+
    | ``%%``        | The character ``%``                   |
    +---------------+---------------------------------------+

- Workers now synchronizes revoked tasks with its neighbors.

    This happens at startup and causes a one second startup delay
    to collect broadcast responses from other workers.

- Workers logical clock value is now persisted so that the clock
  is not reset when a worker restarts.

    The logical clock is also synchronized with other nodes
    in the same cluster (neighbors), so this means that the logical
    epoch will start at the point when the first worker in the cluster
    starts.

    You may notice that the logical clock is an integer value and increases
    very rapidly. It will take several millennia before the clock overflows 64 bits,
    so this is not a concern.

- Message expires value is now forwarded at retry (Issue #980).

    The value is forwarded at is, so the expiry time will not change.
    To update the expiry time you would have to pass the expires
    argument to ``retry()``.

- Worker now crashes if a channel error occurs.

    Channel errors are transport specific and is the list of exceptions
    returned by ``Connection.channel_errors``.
    For RabbitMQ this means that Celery will crash if the equivalence
    checks for one of the queues in :setting:`CELERY_QUEUES` mismatches, which
    makes sense since this is a scenario where manual intervention is
    required.

- Calling ``AsyncResult.get()`` on a chain now propagates errors for previous
  tasks (Issue #1014).

- The parent attribute of ``AsyncResult`` is now reconstructed when using JSON
  serialization (Issue #1014).

- Worker disconnection logs are now logged with severity warning instead of
  error.

    Contributed by Chris Adams.

- The logger named ``celery.concurrency`` has been renamed to ``celery.pool``.

- New command line utility ``celery graph``

    This utility creates graphs in GraphViz dot format.

    You can create graphs from the currently installed bootsteps:

    .. code-block:: bash

        # Create graph of currently installed bootsteps in both the worker
        # and consumer namespaces.
        $ celery graph bootsteps | dot -T png -o steps.png

        # Graph of the consumer namespace only.
        $ celery graph bootsteps consumer | dot -T png -o consumer_only.png

        # Graph of the worker namespace only.
        $ celery graph bootsteps worker | dot -T png -o worker_only.png

    Or graphs of workers in a cluster:

    .. code-block:: bash

        # Create graph from the current cluster
        $ celery graph workers | dot -T png -o workers.png


        # Create graph from a specified list of workers
        $ celery graph workers nodes:w1,w2,w3 | dot -T png workers.png

        # also specify the number of threads in each worker
        $ celery graph workers nodes:w1,w2,w3 threads:2,4,6

        # ...also specify the broker and backend URLs shown in the graph
        $ celery graph workers broker:amqp:// backend:redis://

        # ...also specify the max number of workers/threads shown (wmax/tmax),
        # enumerating anything that exceeds that number.
        $ celery graph workers wmax:10 tmax:3

- Ability to trace imports for debugging purposes.

    The :envvar:`C_IMPDEBUG` can be set to trace imports as they
    occur:

    .. code-block:: bash

        $ C_IMDEBUG=1 celery worker -l info

    .. code-block:: bash

        $ C_IMPDEBUG=1 celery shell


- :class:`celery.apps.worker.Worker` has been refactored as a subclass of
  :class:`celery.worker.WorkController`.

    This removes a lot of duplicate functionality.


- :class:`@events.Receiver` is now a :class:`kombu.mixins.ConsumerMixin`
  subclass.

- ``celery.platforms.PIDFile`` renamed to :class:`celery.platforms.Pidfile`.

- ``celery.results.BaseDictBackend`` has been removed, replaced by
  :class:``celery.results.BaseBackend``.


.. _v310-experimental:

Experimental
============

XXX
---

YYY

.. _v310-removals:

Scheduled Removals
==================

- The ``BROKER_INSIST`` setting is no longer supported.

- The ``CELERY_AMQP_TASK_RESULT_CONNECTION_MAX`` setting is no longer
  supported.

    Use :setting:`BROKER_POOL_LIMIT` instead.

- The ``CELERY_TASK_ERROR_WHITELIST`` setting is no longer supported.

    You should set the :class:`~celery.utils.mail.ErrorMail` attribute
    of the task class instead.  You can also do this using
    :setting:`CELERY_ANNOTATIONS`:

        .. code-block:: python

            from celery import Celery
            from celery.utils.mail import ErrorMail

            class MyErrorMail(ErrorMail):
                whitelist = (KeyError, ImportError)

                def should_send(self, context, exc):
                    return isinstance(exc, self.whitelist)

            app = Celery()
            app.conf.CELERY_ANNOTATIONS = {
                '*': {
                    'ErrorMail': MyErrorMails,
                }
            }

- The ``CELERY_AMQP_TASK_RESULT_EXPIRES`` setting is no longer supported.

    Use :setting:`CELERY_TASK_RESULT_EXPIRES` instead.

- Functions that establishes broker connections no longer
  supports the ``connect_timeout`` argument.

    This can now only be set using the :setting:`BROKER_CONNECTION_TIMEOUT`
    setting.  This is because function rarely establish connections directly,
    but instead acquire connections from the connection pool.

.. _v310-deprecations:

Deprecations
============

See the :ref:`deprecation-timeline`.

- XXX

    YYY


.. _v310-fixes:

Fixes
=====

- XXX

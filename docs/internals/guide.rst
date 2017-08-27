.. _internals-guide:

================================
 Contributors Guide to the Code
================================

.. contents::
    :local:

Philosophy
==========

The API>RCP Precedence Rule
---------------------------

- The API is more important than Readability
- Readability is more important than Convention
- Convention is more important than Performance
    - …unless the code is a proven hot-spot.

More important than anything else is the end-user API.
Conventions must step aside, and any suffering is always alleviated
if the end result is a better API.

Conventions and Idioms Used
===========================

Classes
-------

Naming
~~~~~~

- Follows :pep:`8`.

- Class names must be `CamelCase`.
- but not if they're verbs, verbs shall be `lower_case`:

    .. code-block:: python

        # - test case for a class
        class TestMyClass(Case):                # BAD
            pass

        class test_MyClass(Case):               # GOOD
            pass

        # - test case for a function
        class TestMyFunction(Case):             # BAD
            pass

        class test_my_function(Case):           # GOOD
            pass

        # - "action" class (verb)
        class UpdateTwitterStatus(object):    # BAD
            pass

        class update_twitter_status(object):    # GOOD
            pass

    .. note::

        Sometimes it makes sense to have a class mask as a function,
        and there's precedence for this in the Python standard library (e.g.,
        :class:`~contextlib.contextmanager`). Celery examples include
        :class:`~celery.signature`, :class:`~celery.chord`,
        ``inspect``, :class:`~kombu.utils.functional.promise` and more..

- Factory functions and methods must be `CamelCase` (excluding verbs):

    .. code-block:: python

        class Celery(object):

            def consumer_factory(self):     # BAD
                ...

            def Consumer(self):             # GOOD
                ...

Default values
~~~~~~~~~~~~~~

Class attributes serve as default values for the instance,
as this means that they can be set by either instantiation or inheritance.

**Example:**

.. code-block:: python

    class Producer(object):
        active = True
        serializer = 'json'

        def __init__(self, serializer=None, active=None):
            self.serializer = serializer or self.serializer

            # must check for None when value can be false-y
            self.active = active if active is not None else self.active

A subclass can change the default value:

.. code-block:: python

    TaskProducer(Producer):
        serializer = 'pickle'

and the value can be set at instantiation:

.. code-block:: pycon

    >>> producer = TaskProducer(serializer='msgpack')

Exceptions
~~~~~~~~~~

Custom exceptions raised by an objects methods and properties
should be available as an attribute and documented in the
method/property that throw.

This way a user doesn't have to find out where to import the
exception from, but rather use ``help(obj)`` and access
the exception class from the instance directly.

**Example**:

.. code-block:: python

    class Empty(Exception):
        pass

    class Queue(object):
        Empty = Empty

        def get(self):
            """Get the next item from the queue.

            :raises Queue.Empty: if there are no more items left.

            """
            try:
                return self.queue.popleft()
            except IndexError:
                raise self.Empty()

Composites
~~~~~~~~~~

Similarly to exceptions, composite classes should be override-able by
inheritance and/or instantiation. Common sense can be used when
selecting what classes to include, but often it's better to add one
too many: predicting what users need to override is hard (this has
saved us from many a monkey patch).

**Example**:

.. code-block:: python

    class Worker(object):
        Consumer = Consumer

        def __init__(self, connection, consumer_cls=None):
            self.Consumer = consumer_cls or self.Consumer

        def do_work(self):
            with self.Consumer(self.connection) as consumer:
                self.connection.drain_events()

Applications vs. "single mode"
==============================

In the beginning Celery was developed for Django, simply because
this enabled us get the project started quickly, while also having
a large potential user base.

In Django there's a global settings object, so multiple Django projects
can't co-exist in the same process space, this later posed a problem
for using Celery with frameworks that don't have this limitation.

Therefore the app concept was introduced. When using apps you use 'celery'
objects instead of importing things from Celery sub-modules, this
(unfortunately) also means that Celery essentially has two API's.

Here's an example using Celery in single-mode:

.. code-block:: python

    from celery import task
    from celery.task.control import inspect

    from .models import CeleryStats

    @task
    def write_stats_to_db():
        stats = inspect().stats(timeout=1)
        for node_name, reply in stats:
            CeleryStats.objects.update_stat(node_name, stats)


and here's the same using Celery app objects:

.. code-block:: python

    from .celery import celery
    from .models import CeleryStats

    @app.task
    def write_stats_to_db():
        stats = celery.control.inspect().stats(timeout=1)
        for node_name, reply in stats:
            CeleryStats.objects.update_stat(node_name, stats)


In the example above the actual application instance is imported
from a module in the project, this module could look something like this:

.. code-block:: python

    from celery import Celery

    app = Celery(broker='amqp://')


Module Overview
===============

- celery.app

    This is the core of Celery: the entry-point for all functionality.

- celery.loaders

    Every app must have a loader. The loader decides how configuration
    is read; what happens when the worker starts; when a task starts and ends;
    and so on.

    The loaders included are:

        - app

            Custom Celery app instances uses this loader by default.

        - default

            "single-mode" uses this loader by default.

    Extension loaders also exist, for example :pypi:`celery-pylons`.

- celery.worker

    This is the worker implementation.

- celery.backends

    Task result backends live here.

- celery.apps

    Major user applications: worker and beat.
    The command-line wrappers for these are in celery.bin (see below)

- celery.bin

    Command-line applications.
    :file:`setup.py` creates setuptools entry-points for these.

- celery.concurrency

    Execution pool implementations (prefork, eventlet, gevent, solo).

- celery.db

    Database models for the SQLAlchemy database result backend.
    (should be moved into :mod:`celery.backends.database`)

- celery.events

    Sending and consuming monitoring events, also includes curses monitor,
    event dumper and utilities to work with in-memory cluster state.

- celery.execute.trace

    How tasks are executed and traced by the worker, and in eager mode.

- celery.security

    Security related functionality, currently a serializer using
    cryptographic digests.

- celery.task

    single-mode interface to creating tasks, and controlling workers.

- t.unit (int distribution)

    The unit test suite.

- celery.utils

    Utility functions used by the Celery code base.
    Much of it is there to be compatible across Python versions.

- celery.contrib

    Additional public code that doesn't fit into any other name-space.

Worker overview
===============

* `celery.bin.worker:Worker`

   This is the command-line interface to the worker.

   Responsibilities:
       * Daemonization when :option:`--detach <celery worker --detach>` set,
       * dropping privileges when using :option:`--uid <celery worker --uid>`/
         :option:`--gid <celery worker --gid>` arguments
       * Installs "concurrency patches" (eventlet/gevent monkey patches).

  ``app.worker_main(argv)`` calls
  ``instantiate('celery.bin.worker:Worker')(app).execute_from_commandline(argv)``

* `app.Worker` -> `celery.apps.worker:Worker`

   Responsibilities:
   * sets up logging and redirects standard outs
   * installs signal handlers (`TERM`/`HUP`/`STOP`/`USR1` (cry)/`USR2` (rdb))
   * prints banner and warnings (e.g., pickle warning)
   * handles the :option:`celery worker --purge` argument

* `app.WorkController` -> `celery.worker.WorkController`

   This is the real worker, built up around bootsteps.

.. _guide-monitoring:

=================================
 Monitoring and Management Guide
=================================

.. contents::
    :local:

Introduction
============

There are several tools available to monitor and inspect Celery clusters.

This document describes some of these, as as well as
features related to monitoring, like events and broadcast commands.

.. _monitoring-workers:

Workers
=======

.. _monitoring-celeryctl:


``celery``: Management Command-line Utilities
---------------------------------------------

.. versionadded:: 2.1

:program:`celery` can also be used to inspect
and manage worker nodes (and to some degree tasks).

To list all the commands available do:

.. code-block:: bash

    $ celery help

or to get help for a specific command do:

.. code-block:: bash

    $ celery <command> --help

Commands
~~~~~~~~

* **shell**: Drop into a Python shell.

  The locals will include the ``celery`` variable, which is the current app.
  Also all known tasks will be automatically added to locals (unless the
  ``--without-tasks`` flag is set).

  Uses Ipython, bpython, or regular python in that order if installed.
  You can force an implementation using ``--force-ipython|-I``,
  ``--force-bpython|-B``, or ``--force-python|-P``.

* **status**: List active nodes in this cluster

    .. code-block:: bash

            $ celery status

* **result**: Show the result of a task

    .. code-block:: bash

        $ celery result -t tasks.add 4e196aa4-0141-4601-8138-7aa33db0f577

    Note that you can omit the name of the task as long as the
    task doesn't use a custom result backend.

* **purge**: Purge messages from all configured task queues.

    .. code-block:: bash

        $ celery purge

    .. warning::
        There is no undo for this operation, and messages will
        be permanently deleted!

* **inspect active**: List active tasks

    .. code-block:: bash

        $ celery inspect active

    These are all the tasks that are currently being executed.

* **inspect scheduled**: List scheduled ETA tasks

    .. code-block:: bash

        $ celery inspect scheduled

    These are tasks reserved by the worker because they have the
    `eta` or `countdown` argument set.

* **inspect reserved**: List reserved tasks

    .. code-block:: bash

        $ celery inspect reserved

    This will list all tasks that have been prefetched by the worker,
    and is currently waiting to be executed (does not include tasks
    with an eta).

* **inspect revoked**: List history of revoked tasks

    .. code-block:: bash

        $ celery inspect revoked

* **inspect registered**: List registered tasks

    .. code-block:: bash

        $ celery inspect registered

* **inspect stats**: Show worker statistics

    .. code-block:: bash

        $ celery inspect stats

* **control enable_events**: Enable events

    .. code-block:: bash

        $ celery control enable_events

* **control disable_events**: Disable events

    .. code-block:: bash

        $ celery inspect disable_events

* **migrate**: Migrate tasks from one broker to another (**EXPERIMENTAL**).

    .. code-block:: bash

        $ celery migrate redis://localhost amqp://localhost

  This command will migrate all the tasks on one broker to another.
  As this command is new and experimental you should be sure to have
  a backup of the data before proceeding.

.. note::

    All ``inspect`` commands supports a ``--timeout`` argument,
    This is the number of seconds to wait for responses.
    You may have to increase this timeout if you're not getting a response
    due to latency.

.. _celeryctl-inspect-destination:

Specifying destination nodes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default the inspect commands operates on all workers.
You can specify a single, or a list of workers by using the
`--destination` argument:

.. code-block:: bash

    $ celery inspect -d w1,w2 reserved


.. _monitoring-flower:

Celery Flower: Web interface
----------------------------

Celery Flower is a web based, real-time monitor and administration tool.

Features
~~~~~~~~

- Shutdown or restart workers
- View workers status (completed, running tasks, etc.)
- View worker pool options (timeouts, processes, etc.)
- Control worker pool size
- View message broker options
- View active queues, add or cancel queues
- View processed task stats by type
- View currently running tasks
- View scheduled tasks
- View reserved and revoked tasks
- Apply time and rate limits
- View all active configuration options
- View all tasks (by type, by worker, etc.)
- View all task options (arguments, start time, runtime, etc.)
- Revoke or terminate tasks
- View real-time execution graphs

**Screenshots**

.. figure:: ../images/dashboard.png
   :width: 700px

.. figure:: ../images/monitor.png
   :width: 700px

More screenshots_:

.. _screenshots: https://github.com/mher/flower/tree/master/docs/screenshots

Usage
~~~~~

Install Celery Flower:

.. code-block:: bash

    $ pip install flower

Launch Celery Flower and open http://localhost:8008 in browser:

.. code-block:: bash

    $ celery flower

.. _monitoring-django-admin:

Django Admin Monitor
--------------------

.. versionadded:: 2.1

When you add `django-celery`_ to your Django project you will
automatically get a monitor section as part of the Django admin interface.

This can also be used if you're not using Celery with a Django project.

*Screenshot*

.. figure:: ../images/djangoceleryadmin2.jpg
   :width: 700px

.. _`django-celery`: http://pypi.python.org/pypi/django-celery


.. _monitoring-django-starting:

Starting the monitor
~~~~~~~~~~~~~~~~~~~~

The Celery section will already be present in your admin interface,
but you won't see any data appearing until you start the snapshot camera.

The camera takes snapshots of the events your workers sends at regular
intervals, storing them in your database (See :ref:`monitoring-snapshots`).

To start the camera run:

.. code-block:: bash

    $ python manage.py celerycam

If you haven't already enabled the sending of events you need to do so:

.. code-block:: bash

    $ python manage.py celery control enable_events

:Tip: You can enable events when the worker starts using the `-E` argument.

Now that the camera has been started, and events have been enabled
you should be able to see your workers and the tasks in the admin interface
(it may take some time for workers to show up).

The admin interface shows tasks, worker nodes, and even
lets you perform some actions, like revoking and rate limiting tasks,
or shutting down worker nodes.

.. _monitoring-django-frequency:

Shutter frequency
~~~~~~~~~~~~~~~~~

By default the camera takes a snapshot every second, if this is too frequent
or you want to have higher precision, then you can change this using the
``--frequency`` argument.  This is a float describing how often, in seconds,
it should wake up to check if there are any new events:

.. code-block:: bash

    $ python manage.py celerycam --frequency=3.0

The camera also supports rate limiting using the ``--maxrate`` argument.
While the frequency controls how often the camera thread wakes up,
the rate limit controls how often it will actually take a snapshot.

The rate limits can be specified in seconds, minutes or hours
by appending `/s`, `/m` or `/h` to the value.
Example: ``--maxrate=100/m``, means "hundred writes a minute".

The rate limit is off by default, which means it will take a snapshot
for every ``--frequency`` seconds.

The events also expire after some time, so the database doesn't fill up.
Successful tasks are deleted after 1 day, failed tasks after 3 days,
and tasks in other states after 5 days.

.. _monitoring-django-reset:

Resetting monitor data
~~~~~~~~~~~~~~~~~~~~~~

To reset the monitor data you need to clear out two models::

    >>> from djcelery.models import WorkerState, TaskState

    # delete worker history
    >>> WorkerState.objects.all().delete()

    # delete task history
    >>> TaskState.objects.all().update(hidden=True)
    >>> TaskState.objects.purge()

.. _monitoring-django-expiration:

Expiration
~~~~~~~~~~

By default monitor data for successful tasks will expire in 1 day,
failed tasks in 3 days and pending tasks in 5 days.

You can change the expiry times for each of these using
adding the following settings to your :file:`settings.py`:

.. code-block:: python

    from datetime import timedelta

    CELERYCAM_EXPIRE_SUCCESS = timedelta(hours=1)
    CELERYCAM_EXPIRE_ERROR = timedelta(hours=2)
    CELERYCAM_EXPIRE_PENDING = timedelta(hours=2)

.. _monitoring-nodjango:

Using outside of Django
~~~~~~~~~~~~~~~~~~~~~~~

`django-celery` also installs the :program:`djcelerymon` program. This
can be used by non-Django users, and runs both a web server and a snapshot
camera in the same process.

**Installing**

Using :program:`pip`:

.. code-block:: bash

    $ pip install -U django-celery

or using :program:`easy_install`:

.. code-block:: bash

    $ easy_install -U django-celery

**Running**

:program:`djcelerymon` reads configuration from your Celery configuration
module, and sets up the Django environment using the same settings:

.. code-block:: bash

    $ djcelerymon

Database tables will be created the first time the monitor is run.
By default an `sqlite3` database file named
:file:`djcelerymon.db` is used, so make sure this file is writeable by the
user running the monitor.

If you want to store the events in a different database, e.g. MySQL,
then you can configure the `DATABASE*` settings directly in your Celery
config module.  See http://docs.djangoproject.com/en/dev/ref/settings/#databases
for more information about the database options available.

You will also be asked to create a superuser (and you need to create one
to be able to log into the admin later)::

    Creating table auth_permission
    Creating table auth_group_permissions
    [...]

    You just installed Django's auth system, which means you don't
    have any superusers defined.  Would you like to create
    one now? (yes/no): yes
    Username (Leave blank to use 'username'): username
    Email address: me@example.com
    Password: ******
    Password (again): ******
    Superuser created successfully.

    [...]
    Django version 1.2.1, using settings 'celeryconfig'
    Development server is running at http://127.0.0.1:8000/
    Quit the server with CONTROL-C.

Now that the service is started you can visit the monitor
at http://127.0.0.1:8000, and log in using the user you created.

For a list of the command line options supported by :program:`djcelerymon`,
please see ``djcelerymon --help``.

.. _monitoring-celeryev:

celery events: Curses Monitor
-----------------------------

.. versionadded:: 2.0

`celery events` is a simple curses monitor displaying
task and worker history.  You can inspect the result and traceback of tasks,
and it also supports some management commands like rate limiting and shutting
down workers.

Starting:

.. code-block:: bash

    $ celery events

You should see a screen like:

.. figure:: ../images/celeryevshotsm.jpg


`celery events` is also used to start snapshot cameras (see
:ref:`monitoring-snapshots`:

.. code-block:: bash

    $ celery events --camera=<camera-class> --frequency=1.0

and it includes a tool to dump events to :file:`stdout`:

.. code-block:: bash

    $ celery events --dump

For a complete list of options use ``--help``:

.. code-block:: bash

    $ celery events --help


.. _monitoring-celerymon:

celerymon: Web monitor
----------------------

`celerymon`_ is the ongoing work to create a web monitor.
It's far from complete yet, and does currently only support
a JSON API.  Help is desperately needed for this project, so if you,
or someone you know would like to contribute templates, design, code
or help this project in any way, please get in touch!

:Tip: The Django admin monitor can be used even though you're not using
      Celery with a Django project.  See :ref:`monitoring-nodjango`.

.. _`celerymon`: http://github.com/celery/celerymon/

.. _monitoring-rabbitmq:

RabbitMQ
========

To manage a Celery cluster it is important to know how
RabbitMQ can be monitored.

RabbitMQ ships with the `rabbitmqctl(1)`_ command,
with this you can list queues, exchanges, bindings,
queue lengths, the memory usage of each queue, as well
as manage users, virtual hosts and their permissions.

.. note::

    The default virtual host (``"/"``) is used in these
    examples, if you use a custom virtual host you have to add
    the ``-p`` argument to the command, e.g:
    ``rabbitmqctl list_queues -p my_vhost ....``

.. _`rabbitmqctl(1)`: http://www.rabbitmq.com/man/rabbitmqctl.1.man.html

.. _monitoring-rmq-queues:

Inspecting queues
-----------------

Finding the number of tasks in a queue:

.. code-block:: bash

    $ rabbitmqctl list_queues name messages messages_ready \
                              messages_unacknowledged


Here `messages_ready` is the number of messages ready
for delivery (sent but not received), `messages_unacknowledged`
is the number of messages that has been received by a worker but
not acknowledged yet (meaning it is in progress, or has been reserved).
`messages` is the sum of ready and unacknowledged messages.


Finding the number of workers currently consuming from a queue:

.. code-block:: bash

    $ rabbitmqctl list_queues name consumers

Finding the amount of memory allocated to a queue:

.. code-block:: bash

    $ rabbitmqctl list_queues name memory

:Tip: Adding the ``-q`` option to `rabbitmqctl(1)`_ makes the output
      easier to parse.


.. _monitoring-redis:

Redis
=====

If you're using Redis as the broker, you can monitor the Celery cluster using
the `redis-cli(1)` command to list lengths of queues.

.. _monitoring-redis-queues:

Inspecting queues
-----------------

Finding the number of tasks in a queue:

.. code-block:: bash

    $ redis-cli -h HOST -p PORT -n DATABASE_NUMBER llen QUEUE_NAME

The default queue is named `celery`. To get all available queues, invoke:

.. code-block:: bash

    $ redis-cli -h HOST -p PORT -n DATABASE_NUMBER keys \*

.. note::

  If a list has no elements in Redis, it doesn't exist. Hence it won't show up
  in the `keys` command output. `llen` for that list returns 0 in that case.

  On the other hand, if you're also using Redis for other purposes, the output
  of the `keys` command will include unrelated values stored in the database.
  The recommended way around this is to use a dedicated `DATABASE_NUMBER` for
  Celery.

.. _monitoring-munin:

Munin
=====

This is a list of known Munin plug-ins that can be useful when
maintaining a Celery cluster.

* rabbitmq-munin: Munin plug-ins for RabbitMQ.

    http://github.com/ask/rabbitmq-munin

* celery_tasks: Monitors the number of times each task type has
  been executed (requires `celerymon`).

    http://exchange.munin-monitoring.org/plugins/celery_tasks-2/details

* celery_task_states: Monitors the number of tasks in each state
  (requires `celerymon`).

    http://exchange.munin-monitoring.org/plugins/celery_tasks/details


.. _monitoring-events:

Events
======

The worker has the ability to send a message whenever some event
happens.  These events are then captured by tools like :program:`celerymon`
and :program:`celery events` to monitor the cluster.

.. _monitoring-snapshots:

Snapshots
---------

.. versionadded:: 2.1

Even a single worker can produce a huge amount of events, so storing
the history of all events on disk may be very expensive.

A sequence of events describes the cluster state in that time period,
by taking periodic snapshots of this state you can keep all history, but
still only periodically write it to disk.

To take snapshots you need a Camera class, with this you can define
what should happen every time the state is captured;  You can
write it to a database, send it by email or something else entirely.

:program:`celery events` is then used to take snapshots with the camera,
for example if you want to capture state every 2 seconds using the
camera ``myapp.Camera`` you run :program:`celery events` with the following
arguments:

.. code-block:: bash

    $ celery events -c myapp.Camera --frequency=2.0


.. _monitoring-camera:

Custom Camera
~~~~~~~~~~~~~

Cameras can be useful if you need to capture events and do something
with those events at an interval.  For real-time event processing
you should use :class:`@events.Receiver` directly, like in
:ref:`event-real-time-example`.

Here is an example camera, dumping the snapshot to screen:

.. code-block:: python

    from pprint import pformat

    from celery.events.snapshot import Polaroid

    class DumpCam(Polaroid):

        def on_shutter(self, state):
            if not state.event_count:
                # No new events since last snapshot.
                return
            print('Workers: %s' % (pformat(state.workers, indent=4), ))
            print('Tasks: %s' % (pformat(state.tasks, indent=4), ))
            print('Total: %s events, %s tasks' % (
                state.event_count, state.task_count))

See the API reference for :mod:`celery.events.state` to read more
about state objects.

Now you can use this cam with :program:`celery events` by specifying
it with the :option:`-c` option:

.. code-block:: bash

    $ celery events -c myapp.DumpCam --frequency=2.0

Or you can use it programmatically like this:

.. code-block:: python

    from celery import Celery
    from myapp import DumpCam

    def main(app, freq=1.0):
        state = app.events.State()
        with app.connection() as connection:
            recv = app.events.Receiver(connection, handlers={'*': state.event})
            with DumpCam(state, freq=freq):
                recv.capture(limit=None, timeout=None)

    if __name__ == '__main__':
        celery = Celery(broker='amqp://guest@localhost//')
        main(celery)

.. _event-real-time-example:

Real-time processing
--------------------

To process events in real-time you need the following

- An event consumer (this is the ``Receiver``)

- A set of handlers called when events come in.

    You can have different handlers for each event type,
    or a catch-all handler can be used ('*')

- State (optional)

  :class:`@events.State` is a convenient in-memory representation
  of tasks and workers in the cluster that is updated as events come in.

  It encapsulates solutions for many common things, like checking if a
  worker is still alive (by verifying heartbeats), merging event fields
  together as events come in, making sure timestamps are in sync, and so on.


Combining these you can easily process events in real-time:


.. code-block:: python


    from celery import Celery


    def monitor_events(app):
        state = app.events.State()

        def on_event(event):
            state.event(event)   # <-- updates in-memory cluster state

            print('Workers online: %r' % ', '.join(
                worker for worker in state.workers if worker.alive
            )

        with app.connection() as connection:
            recv = app.events.Receiver(connection, handlers={'*': on_event})
            recv.capture(limit=None, timeout=None, wakeup=True)


.. note::

    The wakeup argument to ``capture`` sends a signal to all workers
    to force them to send a heartbeat.  This way you can immediately see
    workers when the monitor starts.


You can listen to specific events by specifying the handlers:

.. code-block:: python

    from celery import Celery

    def my_monitor(app):
        state = app.events.State()

        def announce_failed_tasks(event):
            state.event(event)
            task_id = event['uuid']

            print('TASK FAILED: %s[%s] %s' % (
                event['name'], task_id, state[task_id].info(), ))

        def announce_dead_workers(event):
            state.event(event)
            hostname = event['hostname']

            if not state.workers[hostname].alive:
                print('Worker %s missed heartbeats' % (hostname, ))


        with app.connection() as connection:
            recv = app.events.Receiver(connection, handlers={
                    'task-failed': announce_failed_tasks,
                    'worker-heartbeat': announce_dead_workers,
            })
            recv.capture(limit=None, timeout=None, wakeup=True)

    if __name__ == '__main__':
        celery = Celery(broker='amqp://guest@localhost//')
        my_monitor(celery)



.. _event-reference:

Event Reference
===============

This list contains the events sent by the worker, and their arguments.

.. _event-reference-task:

Task Events
-----------

.. event:: task-sent

task-sent
~~~~~~~~~

:signature: ``task-sent(uuid, name, args, kwargs, retries, eta, expires,
              queue, exchange, routing_key)``

Sent when a task message is published and
the :setting:`CELERY_SEND_TASK_SENT_EVENT` setting is enabled.

.. event:: task-received

task-received
~~~~~~~~~~~~~

:signature: ``task-received(uuid, name, args, kwargs, retries, eta, hostname,
              timestamp)``

Sent when the worker receives a task.

.. event:: task-started

task-started
~~~~~~~~~~~~

:signature: ``task-started(uuid, hostname, timestamp, pid)``

Sent just before the worker executes the task.

.. event:: task-succeeded

task-succeeded
~~~~~~~~~~~~~~

:signature: ``task-succeeded(uuid, result, runtime, hostname, timestamp)``

Sent if the task executed successfully.

Runtime is the time it took to execute the task using the pool.
(Starting from the task is sent to the worker pool, and ending when the
pool result handler callback is called).

.. event:: task-failed

task-failed
~~~~~~~~~~~

:signature: ``task-failed(uuid, exception, traceback, hostname, timestamp)``

Sent if the execution of the task failed.

.. event:: task-revoked

task-revoked
~~~~~~~~~~~~

:signature: ``task-revoked(uuid, terminated, signum, expired)``

Sent if the task has been revoked (Note that this is likely
to be sent by more than one worker).

- ``terminated`` is set to true if the task process was terminated,
    and the ``signum`` field set to the signal used.

- ``expired`` is set to true if the task expired.

.. event:: task-retried

task-retried
~~~~~~~~~~~~

:signature: ``task-retried(uuid, exception, traceback, hostname, timestamp)``

Sent if the task failed, but will be retried in the future.

.. _event-reference-worker:

Worker Events
-------------

.. event:: worker-online

worker-online
~~~~~~~~~~~~~

:signature: ``worker-online(hostname, timestamp, freq, sw_ident, sw_ver, sw_sys)``

The worker has connected to the broker and is online.

- `hostname`: Hostname of the worker.
- `timestamp`: Event timestamp.
- `freq`: Heartbeat frequency in seconds (float).
- `sw_ident`: Name of worker software (e.g. ``py-celery``).
- `sw_ver`: Software version (e.g. 2.2.0).
- `sw_sys`: Operating System (e.g. Linux, Windows, Darwin).

.. event:: worker-heartbeat

worker-heartbeat
~~~~~~~~~~~~~~~~

:signature: ``worker-heartbeat(hostname, timestamp, freq, sw_ident, sw_ver, sw_sys,
              active, processed)``

Sent every minute, if the worker has not sent a heartbeat in 2 minutes,
it is considered to be offline.

- `hostname`: Hostname of the worker.
- `timestamp`: Event timestamp.
- `freq`: Heartbeat frequency in seconds (float).
- `sw_ident`: Name of worker software (e.g. ``py-celery``).
- `sw_ver`: Software version (e.g. 2.2.0).
- `sw_sys`: Operating System (e.g. Linux, Windows, Darwin).
- `active`: Number of currently executing tasks.
- `processed`: Total number of tasks processed by this worker.

.. event:: worker-offline

worker-offline
~~~~~~~~~~~~~~

:signature: ``worker-offline(hostname, timestamp, freq, sw_ident, sw_ver, sw_sys)``

The worker has disconnected from the broker.

==================
 Monitoring Guide
==================

.. contents::
    :local:

Introduction
============

There are several tools available to monitor and inspect Celery clusters.
This document describes some of these, as as well as
features related to monitoring, like events and broadcast commands.


Monitoring and Inspecting Workers
=================================

celeryctl
---------

* Listing active nodes in the cluster
    ::

    $ celeryctl status

* Show the result of a task
    ::

        $ celeryctl -t tasks.add 4e196aa4-0141-4601-8138-7aa33db0f577

    Note that you can omit the name of the task as long as the
    task doesn't use a custom result backend.

* Listing all tasks that are currently being executed
    ::

        $ celeryctl inspect active

* Listing scheduled ETA tasks
    ::

        $ celeryctl inspect scheduled

    These are tasks reserved by the worker because they have the
    ``eta`` or ``countdown`` argument set.

* Listing reserved tasks
    ::

        $ celeryctl inspect reserved

    This will list all tasks that have been prefetched by the worker,
    and is currently waiting to be executed (does not include tasks
    with an eta).

* Listing the history of revoked tasks
    ::

        $ celeryctl inspect revoked

* Show registered tasks
    ::

        $ celeryctl inspect registered_tasks

* Showing statistics
    ::

        $ celeryctl inspect stats

* Diagnosing the worker pools
    ::

        $ celeryctl inspect diagnose

    This will verify that the workers pool processes are available
    to do work, note that this will not work if the worker is busy.

* Enabling/disabling events
    ::

        $ celeryctl inspect enable_events
        $ celeryctl inspect disable_events


By default the inspect commands operates on all workers.
You can specify a single, or a list of workers by using the
``--destination`` argument::

    $ celeryctl inspect -d w1,w2 reserved


:Note: All ``inspect`` commands supports the ``--timeout`` argument,
       which is the number of seconds to wait for responses.
       You may have to increase this timeout If you're getting empty responses
       due to latency.

Django Admin
------------

TODO

celeryev
--------

TODO

celerymon
---------

TODO

Monitoring and inspecting RabbitMQ
==================================

To manage a Celery cluster it is important to know how
RabbitMQ can be monitored.

RabbitMQ ships with the `rabbitmqctl(1)`_ command,
with this you can list queues, exchanges, bindings,
queue lenghts, the memory usage of each queue, as well
as manage users, virtual hosts and their permissions.

:Note: The default virtual host (``"/"``) is used in these
       examples, if you use a custom virtual host you have to add
       the ``-p`` argument to the command, e.g:
       ``rabbitmqctl list_queues -p my_vhost ....``


.. _`rabbitmqctl(1)`: http://www.rabbitmq.com/man/rabbitmqctl.1.man.html

Inspecting queues
-----------------

Finding the number of tasks in a queue::


    $ rabbitmqctl list_queues name messages messages_ready \
                              messages_unacknowlged


Here ``messages_ready`` is the number of messages ready
for delivery (sent but not received), ``messages_unacknowledged``
is the number of messages that has been received by a worker but
not acknowledged yet (meaning it is in progress, or has been reserved).
``messages`` is the sum of ready and unacknowledged messages combined.


Finding the number of workers currently consuming from a queue::

    $ rabbitmqctl list_queues name consumers

Finding the amount of memory allocated to a queue::

    $ rabbitmqctl list_queues name memory

:Tip: Adding the ``-q`` option to `rabbitmqctl(1)`_ makes the output
      easier to parse.

Munin
=====

This is a list of known Munin plugins that can be useful when
maintaining a Celery cluster.

* rabbitmq-munin: Munin-plugins for RabbitMQ.

    http://github.com/ask/rabbitmq-munin

* celery_tasks: Monitors the number of times each task type has
  been executed (requires ``celerymon``).

    http://exchange.munin-monitoring.org/plugins/celery_tasks-2/details

* celery_task_states: Monitors the number of tasks in each state
  (requires ``celerymon``).

    http://exchange.munin-monitoring.org/plugins/celery_tasks/details

Events
======

The worker has the ability to send a message whenever some event
happens. These events are then captured by tools like ``celerymon`` and 
``celeryev`` to monitor the cluster.

Snapshots
---------

Even a single worker can produce a huge amount of events, so storing
the history of these events on disk may be hard.

A sequence of events describes the cluster state in that time period,
by taking periodic snapshots of this state we can capture all interesting
information, but only periodically write it to disk.

To take snapshots you need a Camera class, with this you can define
what should happen every time the state is captured. You can
write it to a database, send it by e-mail or something else entirely).

``celeryev`` is then used to take snapshots with the camera,
for example if you want to capture state every 2 seconds using the
camera ``myapp.Camera`` you run ``celeryev`` with the following arguments::

    $ celeryev -c myapp.Camera --frequency=2.0

Custom Camera
~~~~~~~~~~~~~

Here is an example camera that is simply dumping the snapshot to the screen:

.. code-block:: python

    from pprint import pformat

    from celery.events.snapshot import Polaroid

    class DumpCam(Polaroid):

        def shutter(self, state):
            if not state.event_count:
                # No new events since last snapshot.
                return
            print("Workers: %s" % (pformat(state.workers, indent=4), ))
            print("Tasks: %s" % (pformat(state.tasks, indent=4), ))
            print("Total: %s events, %s tasks" % (
                state.event_count, state.task_count))

Now you can use this cam with ``celeryev`` by specifying
it with the ``-c`` option::

    $ celeryev -c myapp.DumpCam --frequency=2.0

Or you can use it programatically like this::

    from celery.events import EventReceiver
    from celery.messaging import establish_connection
    from celery.events.state import State
    from myapp import DumpCam

    def main():
        state = State()
        with establish_connection() as connection:
            recv = EventReceiver(connection, handlers={"*": state.event})
            with DumpCam(state, freq=1.0):
                recv.capture(limit=None, timeout=None)

    if __name__ == "__main__":
        main()

Event Reference
---------------

This list contains the events sent by the worker, and their arguments.

Task Events
~~~~~~~~~~~

* ``task-received(uuid, name, args, kwargs, retries, eta, hostname,
  timestamp)``

    Sent when the worker receives a task.

* ``task-started(uuid, hostname, timestamp)``

    Sent just before the worker executes the task.

* ``task-succeeded(uuid, result, runtime, hostname, timestamp)``

    Sent if the task executed successfully.
    Runtime is the time it took to execute the task using the pool.
    (Time starting from the task is sent to the pool, and ending when the
    pool result handlers callback is called).

* ``task-failed(uuid, exception, traceback, hostname, timestamp)``

    Sent if the execution of the task failed.

* ``task-revoked(uuid)``

    Sent if the task has been revoked (Note that this is likely
    to be sent by more than one worker)

* ``task-retried(uuid, exception, traceback, hostname, delay, timestamp)``

    Sent if the task failed, but will be retried in the future.
    (**NOT IMPLEMENTED**)

Worker Events
~~~~~~~~~~~~~

* ``worker-online(hostname, timestamp)``

    The worker has connected to the broker and is online.

* ``worker-heartbeat(hostname, timestamp)``

    Sent every minute, if the worker has not sent a heartbeat in 2 minutes,
    it is considered to be offline.

* ``worker-offline(hostname, timestamp)``

    The worker has disconnected from the broker.

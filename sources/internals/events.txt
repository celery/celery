=======================
 List of Worker Events
=======================

This is the list of events sent by the worker.
The monitor uses these to visualize the state of the cluster.

.. contents::
    :local:


Task Events
===========

* task-received(uuid, name, args, kwargs, retries, eta, hostname, timestamp)

    Sent when the worker receives a task.

* task-started(uuid, hostname, timestamp)

    Sent just before the worker executes the task.

* task-succeeded(uuid, result, runtime, hostname, timestamp)

    Sent if the task executed successfully.
    Runtime is the time it took to execute the task using the pool.
    (Time starting from the task is sent to the pool, and ending when the
    pool result handlers callback is called).

* task-failed(uuid, exception, traceback, hostname, timestamp)

    Sent if the execution of the task failed.

* task-revoked(uuid)

    Sent if the task has been revoked (Note that this is likely
    to be sent by more than one worker)

* task-retried(uuid, exception, traceback, hostname, timestamp)

    Sent if the task failed, but will be retried.

Worker Events
=============

* worker-online(hostname, timestamp)

    The worker has connected to the broker and is online.

* worker-heartbeat(hostname, timestamp)

    Sent every minute, if the worker has not sent a heartbeat in 2 minutes,
    it's considered to be offline.

* worker-offline(hostname, timestamp)

    The worker has disconnected from the broker.

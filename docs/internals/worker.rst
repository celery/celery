.. _internals-worker:

=======================
 Internals: The worker
=======================

.. contents::
    :local:

Introduction
============

The worker consists of 4 main components: the consumer, the scheduler,
the mediator and the task pool. All these components runs in parallel working
with two data structures: the ready queue and the ETA schedule.

.. image:: ../images/Celery1.0-inside-worker.jpg

Data structures
===============

ready_queue
-----------

The ready queue is either an instance of :class:`Queue.Queue`, or
:class:`celery.buckets.TaskBucket`.  The latter if rate limiting is enabled.

eta_schedule
------------

The ETA schedule is a heap queue sorted by time.


Components
==========

Consumer
--------

Receives messages from the broker using `Kombu`_.

.. _`Kombu`: http://pypi.python.org/pypi/kombu

When a message is received it's converted into a
:class:`celery.worker.job.TaskRequest` object.

Tasks with an ETA are entered into the `eta_schedule`, messages that can
be immediately processed are moved directly to the `ready_queue`.

ScheduleController
------------------

The schedule controller is running the `eta_schedule`.
If the scheduled tasks eta has passed it is moved to the `ready_queue`,
otherwise the thread sleeps until the eta is met (remember that the schedule
is sorted by time).

Mediator
--------
The mediator simply moves tasks in the `ready_queue` over to the
task pool for execution using
:meth:`celery.worker.job.TaskRequest.execute_using_pool`.

TaskPool
--------

This is a slightly modified :class:`multiprocessing.Pool`.
It mostly works the same way, except it makes sure all of the workers
are running at all times. If a worker is missing, it replaces
it with a new one.

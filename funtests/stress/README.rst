=========================
 Celery Stresstest Suite
=========================

.. contents::
    :local:

Introduction
============

These tests will attempt to break the worker in different ways.

The worker must currently be started separately, and it's encouraged
to run the stresstest with different configuration values.

Ideas include:

1)  Frequent maxtasksperchild, single process

::

    $ celery -A stress worker -c 1 --maxtasksperchild=1

2) Frequent scale down & maxtasksperchild, single process

::

    $ AUTOSCALE_KEEPALIVE=0.01 celery -A stress worker --autoscale=1,0 \
                                                       --maxtasksperchild=1

3) Frequent maxtasksperchild, multiple processes

::

    $ celery -A stress worker -c 8 --maxtasksperchild=1``

4) Default, single process

::

    $ celery -A stress worker -c 1

5) Default, multiple processes

::

    $ celery -A stress worker -c 8

6) Processes termianted by time limits

::

    $ celery -A stress worker --time-limit=1

7) Frequent maxtasksperchild, single process with late ack.

::

    $ celery -A stress worker -c1 --maxtasksperchild=1 -- celery.acks_late=1


It's a good idea to include the ``--purge`` argument to clear out tasks from
previous runs.

Note that the stress client will probably hang if the test fails, so this
test suite is currently not suited for automatic runs.

Running the client
------------------

After the worker is running you can start the client to run the complete test
suite::

    $ python -m stress

You can also specify which tests to run:

    $ python -m stress revoketermfast revoketermslow

Or you can start from an offset, e.g. to skip the two first tests use
``--offset=2``::

    $ python -m stress --offset=2

See ``python -m stress --help`` for a list of all available options.


Options
=======

Using a different result backend
--------------------------------

You can set the environment variable ``CSTRESS_BACKEND`` to change
the result backend used::

    $ CSTRESS_BACKEND='amqp://' celery -A stress worker #...
    $ CSTRESS_BACKEND='amqp://' python -m stress

Using a custom queue
--------------------

A queue named ``c.stress`` is created and used by default,
but you can change the name of this queue using the ``CSTRESS_QUEUE``
environment variable.

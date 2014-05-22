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

    $ celery -A stress worker -c1 --maxtasksperchild=1 -Z acks_late


8) Worker using eventlet pool.

    Start the worker::

        $ celery -A stress worker -c1000 -P eventlet

    Then must use the `-g green` test group::

        $ python -m stress -g green

9) Worker using gevent pool.

It's also a good idea to include the ``--purge`` argument to clear out tasks from
previous runs.

Note that the stress client will probably hang if the test fails, so this
test suite is currently not suited for automatic runs.

Configuration Templates
-----------------------

You can select a configuration template using the `-Z` command-line argument
to any :program:`celery -A stress` command or the :program:`python -m stress`
command when running the test suite itself.

The templates available are:

* default

    Using amqp as a broker and rpc as a result backend,
    and also using json for task and result messages.

* redis

    Using redis as a broker and result backend

* acks_late

    Enables late ack globally.

* pickle

    Using pickle as the serializer for tasks and results
    (also allowing the worker to receive and process pickled messages)


You can see the resulting configuration from any template by running
the command::

    $ celery -A stress report -Z redis


Example running the stress test using the ``redis`` configuration template::

    $ python -m stress -Z redis

Example running the worker using the ``redis`` configuration template::

    $ celery -A stress worker -Z redis


You can also mix several templates by listing them separated by commas::

    $ celery -A stress worker -Z redis,acks_late

In this example (``redis,acks_late``) the ``redis`` template will be used
as a configuration, and then additional keys from the ``acks_late`` template
will be added on top as changes::

    $ celery -A stress report -Z redis,acks_late,pickle

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

Using a different broker
------------------------
You can set the environment ``CSTRESS_BROKER`` to change the broker used::

    $ CSTRESS_BROKER='amqp://' celery -A stress worker # …
    $ CSTRESS_BROKER='amqp://' python -m stress

Using a different result backend
--------------------------------

You can set the environment variable ``CSTRESS_BACKEND`` to change
the result backend used::

    $ CSTRESS_BACKEND='amqp://' celery -A stress worker # …
    $ CSTRESS_BACKEND='amqp://' python -m stress

Using a custom queue
--------------------

A queue named ``c.stress`` is created and used by default,
but you can change the name of this queue using the ``CSTRESS_QUEUE``
environment variable.

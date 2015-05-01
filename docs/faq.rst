.. _faq:

============================
 Frequently Asked Questions
============================

.. contents::
    :local:

.. _faq-general:

General
=======

.. _faq-when-to-use:

What kinds of things should I use Celery for?
---------------------------------------------

**Answer:** `Queue everything and delight everyone`_ is a good article
describing why you would use a queue in a web context.

.. _`Queue everything and delight everyone`:
    http://decafbad.com/blog/2008/07/04/queue-everything-and-delight-everyone

These are some common use cases:

* Running something in the background. For example, to finish the web request
  as soon as possible, then update the users page incrementally.
  This gives the user the impression of good performance and "snappiness", even
  though the real work might actually take some time.

* Running something after the web request has finished.

* Making sure something is done, by executing it asynchronously and using
  retries.

* Scheduling periodic work.

And to some degree:

* Distributed computing.

* Parallel execution.

.. _faq-misconceptions:

Misconceptions
==============

.. _faq-loc:

Does Celery really consist of 50.000 lines of code?
---------------------------------------------------

**Answer:** No, this and similarly large numbers have
been reported at various locations.

The numbers as of this writing are:

    - core: 7,141 lines of code.
    - tests: 14,209 lines.
    - backends, contrib, compat utilities: 9,032 lines.

Lines of code is not a useful metric, so
even if Celery did consist of 50k lines of code you would not
be able to draw any conclusions from such a number.

Does Celery have many dependencies?
-----------------------------------

A common criticism is that Celery uses too many dependencies.
The rationale behind such a fear is hard to imagine, especially considering
code reuse as the established way to combat complexity in modern software
development, and that the cost of adding dependencies is very low now
that package managers like pip and PyPI makes the hassle of installing
and maintaining dependencies a thing of the past.

Celery has replaced several dependencies along the way, and
the current list of dependencies are:

celery
~~~~~~

- `kombu`_

Kombu is part of the Celery ecosystem and is the library used
to send and receive messages.  It is also the library that enables
us to support many different message brokers.  It is also used by the
OpenStack project, and many others, validating the choice to separate
it from the Celery codebase.

.. _`kombu`: http://pypi.python.org/pypi/kombu

- `billiard`_

Billiard is a fork of the Python multiprocessing module containing
many performance and stability improvements.  It is an eventual goal
that these improvements will be merged back into Python one day.

It is also used for compatibility with older Python versions
that don't come with the multiprocessing module.

.. _`billiard`: http://pypi.python.org/pypi/billiard

- `pytz`

The pytz module provides timezone definitions and related tools.

.. _`pytz`: http://pypi.python.org/pypi/pytz

django-celery
~~~~~~~~~~~~~

If you use django-celery then you don't have to install celery separately,
as it will make sure that the required version is installed.

django-celery does not have any other dependencies.

kombu
~~~~~

Kombu depends on the following packages:

- `amqp`_

The underlying pure-Python amqp client implementation.  AMQP being the default
broker this is a natural dependency.

.. _`amqp`: http://pypi.python.org/pypi/amqp

.. note::

    To handle the dependencies for popular configuration
    choices Celery defines a number of "bundle" packages,
    see :ref:`bundles`.


.. _faq-heavyweight:

Is Celery heavy-weight?
-----------------------

Celery poses very little overhead both in memory footprint and
performance.

But please note that the default configuration is not optimized for time nor
space, see the :ref:`guide-optimizing` guide for more information.

.. _faq-serializion-is-a-choice:

Is Celery dependent on pickle?
------------------------------

**Answer:** No.

Celery can support any serialization scheme and has built-in support for
JSON, YAML, Pickle and msgpack. Also, as every task is associated with a
content type, you can even send one task using pickle, and another using JSON.

The default serialization format is pickle simply because it is
convenient (it supports sending complex Python objects as task arguments).

If you need to communicate with other languages you should change
to a serialization format that is suitable for that.

You can set a global default serializer, the default serializer for a
particular Task, or even what serializer to use when sending a single task
instance.

.. _faq-is-celery-for-django-only:

Is Celery for Django only?
--------------------------

**Answer:** No.

You can use Celery with any framework, web or otherwise.

.. _faq-is-celery-for-rabbitmq-only:

Do I have to use AMQP/RabbitMQ?
-------------------------------

**Answer**: No.

Although using RabbitMQ is recommended you can also use Redis.  There are also
experimental transports available such as MongoDB, Beanstalk, CouchDB, or using
SQL databases. See :ref:`brokers` for more information.

The experimental transports may have reliability problems and
limited broadcast and event functionality.
For example remote control commands only works with AMQP and Redis.

Redis or a database won't perform as well as
an AMQP broker. If you have strict reliability requirements you are
encouraged to use RabbitMQ or another AMQP broker. Some transports also uses
polling, so they are likely to consume more resources. However, if you for
some reason are not able to use AMQP, feel free to use these alternatives.
They will probably work fine for most use cases, and note that the above
points are not specific to Celery; If using Redis/database as a queue worked
fine for you before, it probably will now. You can always upgrade later
if you need to.

.. _faq-is-celery-multilingual:

Is Celery multilingual?
------------------------

**Answer:** Yes.

:mod:`~celery.bin.worker` is an implementation of Celery in Python. If the
language has an AMQP client, there shouldn't be much work to create a worker
in your language.  A Celery worker is just a program connecting to the broker
to process messages.

Also, there's another way to be language independent, and that is to use REST
tasks, instead of your tasks being functions, they're URLs. With this
information you can even create simple web servers that enable preloading of
code. See: :ref:`User Guide: Remote Tasks <guide-webhooks>`.

.. _faq-troubleshooting:

Troubleshooting
===============

.. _faq-mysql-deadlocks:

MySQL is throwing deadlock errors, what can I do?
-------------------------------------------------

**Answer:** MySQL has default isolation level set to `REPEATABLE-READ`,
if you don't really need that, set it to `READ-COMMITTED`.
You can do that by adding the following to your :file:`my.cnf`::

    [mysqld]
    transaction-isolation = READ-COMMITTED

For more information about InnoDB`s transaction model see `MySQL - The InnoDB
Transaction Model and Locking`_ in the MySQL user manual.

(Thanks to Honza Kral and Anton Tsigularov for this solution)

.. _`MySQL - The InnoDB Transaction Model and Locking`: http://dev.mysql.com/doc/refman/5.1/en/innodb-transaction-model.html

.. _faq-worker-hanging:

The worker is not doing anything, just hanging
----------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.
            or `Why is Task.delay/apply\* just hanging?`.

.. _faq-results-unreliable:

Task results aren't reliably returning
--------------------------------------

**Answer:** If you're using the database backend for results, and in particular
using MySQL, see `MySQL is throwing deadlock errors, what can I do?`_.

.. _faq-publish-hanging:

Why is Task.delay/apply\*/the worker just hanging?
--------------------------------------------------

**Answer:** There is a bug in some AMQP clients that will make it hang if
it's not able to authenticate the current user, the password doesn't match or
the user does not have access to the virtual host specified. Be sure to check
your broker logs (for RabbitMQ that is :file:`/var/log/rabbitmq/rabbit.log` on
most systems), it usually contains a message describing the reason.

.. _faq-worker-on-freebsd:

Does it work on FreeBSD?
------------------------

**Answer:** Depends

When using the RabbitMQ (AMQP) and Redis transports it should work
out of the box.

For other transports the compatibility prefork pool is
used which requires a working POSIX semaphore implementation,
this is enabled in FreeBSD by default since FreeBSD 8.x.
For older version of FreeBSD, you have to enable
POSIX semaphores in the kernel and manually recompile billiard.

Luckily, Viktor Petersson has written a tutorial to get you started with
Celery on FreeBSD here:
http://www.playingwithwire.com/2009/10/how-to-get-celeryd-to-work-on-freebsd/

.. _faq-duplicate-key-errors:

I'm having `IntegrityError: Duplicate Key` errors. Why?
---------------------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.
Thanks to howsthedotcom.

.. _faq-worker-stops-processing:

Why aren't my tasks processed?
------------------------------

**Answer:** With RabbitMQ you can see how many consumers are currently
receiving tasks by running the following command:

.. code-block:: bash

    $ rabbitmqctl list_queues -p <myvhost> name messages consumers
    Listing queues ...
    celery     2891    2

This shows that there's 2891 messages waiting to be processed in the task
queue, and there are two consumers processing them.

One reason that the queue is never emptied could be that you have a stale
worker process taking the messages hostage. This could happen if the worker
wasn't properly shut down.

When a message is received by a worker the broker waits for it to be
acknowledged before marking the message as processed. The broker will not
re-send that message to another consumer until the consumer is shut down
properly.

If you hit this problem you have to kill all workers manually and restart
them::

    ps auxww | grep celeryd | awk '{print $2}' | xargs kill

You might have to wait a while until all workers have finished the work they're
doing. If it's still hanging after a long time you can kill them by force
with::

    ps auxww | grep celeryd | awk '{print $2}' | xargs kill -9

.. _faq-task-does-not-run:

Why won't my Task run?
----------------------

**Answer:** There might be syntax errors preventing the tasks module being imported.

You can find out if Celery is able to run the task by executing the
task manually:

    >>> from myapp.tasks import MyPeriodicTask
    >>> MyPeriodicTask.delay()

Watch the workers log file to see if it's able to find the task, or if some
other error is happening.

.. _faq-periodic-task-does-not-run:

Why won't my periodic task run?
-------------------------------

**Answer:** See `Why won't my Task run?`_.

.. _faq-purge-the-queue:

How do I purge all waiting tasks?
---------------------------------

**Answer:** You can use the ``celery purge`` command to purge
all configured task queues:

.. code-block:: bash

    $ celery -A proj purge

or programatically:

.. code-block:: python

    >>> from proj.celery import app
    >>> app.control.purge()
    1753

If you only want to purge messages from a specific queue
you have to use the AMQP API or the :program:`celery amqp` utility:

.. code-block:: bash

    $ celery -A proj amqp queue.purge <queue name>

The number 1753 is the number of messages deleted.

You can also start :mod:`~celery.bin.worker` with the
:option:`--purge` argument, to purge messages when the worker starts.

.. _faq-messages-left-after-purge:

I've purged messages, but there are still messages left in the queue?
---------------------------------------------------------------------

**Answer:** Tasks are acknowledged (removed from the queue) as soon
as they are actually executed. After the worker has received a task, it will
take some time until it is actually executed, especially if there are a lot
of tasks already waiting for execution. Messages that are not acknowledged are
held on to by the worker until it closes the connection to the broker (AMQP
server). When that connection is closed (e.g. because the worker was stopped)
the tasks will be re-sent by the broker to the next available worker (or the
same worker when it has been restarted), so to properly purge the queue of
waiting tasks you have to stop all the workers, and then purge the tasks
using :func:`celery.control.purge`.

.. _faq-results:

Results
=======

.. _faq-get-result-by-task-id:

How do I get the result of a task if I have the ID that points there?
----------------------------------------------------------------------

**Answer**: Use `task.AsyncResult`::

    >>> result = my_task.AsyncResult(task_id)
    >>> result.get()

This will give you a :class:`~celery.result.AsyncResult` instance
using the tasks current result backend.

If you need to specify a custom result backend, or you want to use
the current application's default backend you can use
:class:`@AsyncResult`:

    >>> result = app.AsyncResult(task_id)
    >>> result.get()

.. _faq-security:

Security
========

Isn't using `pickle` a security concern?
----------------------------------------

**Answer**: Yes, indeed it is.

You are right to have a security concern, as this can indeed be a real issue.
It is essential that you protect against unauthorized
access to your broker, databases and other services transmitting pickled
data.

Note that this is not just something you should be aware of with Celery, for
example also Django uses pickle for its cache client.

For the task messages you can set the :setting:`CELERY_TASK_SERIALIZER`
setting to "json" or "yaml" instead of pickle.

Similarly for task results you can set :setting:`CELERY_RESULT_SERIALIZER`.

For more details of the formats used and the lookup order when
checking which format to use for a task see :ref:`calling-serializers`

Can messages be encrypted?
--------------------------

**Answer**: Some AMQP brokers supports using SSL (including RabbitMQ).
You can enable this using the :setting:`BROKER_USE_SSL` setting.

It is also possible to add additional encryption and security to messages,
if you have a need for this then you should contact the :ref:`mailing-list`.

Is it safe to run :program:`celery worker` as root?
---------------------------------------------------

**Answer**: No!

We're not currently aware of any security issues, but it would
be incredibly naive to assume that they don't exist, so running
the Celery services (:program:`celery worker`, :program:`celery beat`,
:program:`celeryev`, etc) as an unprivileged user is recommended.

.. _faq-brokers:

Brokers
=======

Why is RabbitMQ crashing?
-------------------------

**Answer:** RabbitMQ will crash if it runs out of memory. This will be fixed in a
future release of RabbitMQ. please refer to the RabbitMQ FAQ:
http://www.rabbitmq.com/faq.html#node-runs-out-of-memory

.. note::

    This is no longer the case, RabbitMQ versions 2.0 and above
    includes a new persister, that is tolerant to out of memory
    errors. RabbitMQ 2.1 or higher is recommended for Celery.

    If you're still running an older version of RabbitMQ and experience
    crashes, then please upgrade!

Misconfiguration of Celery can eventually lead to a crash
on older version of RabbitMQ. Even if it doesn't crash, this
can still consume a lot of resources, so it is very
important that you are aware of the common pitfalls.

* Events.

Running :mod:`~celery.bin.worker` with the :option:`-E`/:option:`--events`
option will send messages for events happening inside of the worker.

Events should only be enabled if you have an active monitor consuming them,
or if you purge the event queue periodically.

* AMQP backend results.

When running with the AMQP result backend, every task result will be sent
as a message. If you don't collect these results, they will build up and
RabbitMQ will eventually run out of memory.

Results expire after 1 day by default.  It may be a good idea
to lower this value by configuring the :setting:`CELERY_TASK_RESULT_EXPIRES`
setting.

If you don't use the results for a task, make sure you set the
`ignore_result` option:

.. code-block python

    @app.task(ignore_result=True)
    def mytask():
        …

    class MyTask(Task):
        ignore_result = True

.. _faq-use-celery-with-stomp:

Can I use Celery with ActiveMQ/STOMP?
-------------------------------------

**Answer**: No.  It used to be supported by Carrot,
but is not currently supported in Kombu.

.. _faq-non-amqp-missing-features:

What features are not supported when not using an AMQP broker?
--------------------------------------------------------------

This is an incomplete list of features not available when
using the virtual transports:

    * Remote control commands (supported only by Redis).

    * Monitoring with events may not work in all virtual transports.

    * The `header` and `fanout` exchange types
        (`fanout` is supported by Redis).

.. _faq-tasks:

Tasks
=====

.. _faq-tasks-connection-reuse:

How can I reuse the same connection when calling tasks?
-------------------------------------------------------

**Answer**: See the :setting:`BROKER_POOL_LIMIT` setting.
The connection pool is enabled by default since version 2.5.

.. _faq-sudo-subprocess:

Sudo in a :mod:`subprocess` returns :const:`None`
-------------------------------------------------

There is a sudo configuration option that makes it illegal for process
without a tty to run sudo::

    Defaults requiretty

If you have this configuration in your :file:`/etc/sudoers` file then
tasks will not be able to call sudo when the worker is running as a daemon.
If you want to enable that, then you need to remove the line from sudoers.

See: http://timelordz.com/wiki/Apache_Sudo_Commands

.. _faq-deletes-unknown-tasks:

Why do workers delete tasks from the queue if they are unable to process them?
------------------------------------------------------------------------------
**Answer**:

The worker rejects unknown tasks, messages with encoding errors and messages
that don't contain the proper fields (as per the task message protocol).

If it did not reject them they could be redelivered again and again,
causing a loop.

Recent versions of RabbitMQ has the ability to configure a dead-letter
queue for exchange, so that rejected messages is moved there.

.. _faq-execute-task-by-name:

Can I call a task by name?
-----------------------------

**Answer**: Yes. Use :meth:`@send_task`.
You can also call a task by name from any language
that has an AMQP client.

    >>> app.send_task('tasks.add', args=[2, 2], kwargs={})
    <AsyncResult: 373550e8-b9a0-4666-bc61-ace01fa4f91d>

.. _faq-get-current-task-id:

How can I get the task id of the current task?
----------------------------------------------

**Answer**: The current id and more is available in the task request::

    @app.task(bind=True)
    def mytask(self):
        cache.set(self.request.id, "Running")

For more information see :ref:`task-request-info`.

.. _faq-custom-task-ids:

Can I specify a custom task_id?
-------------------------------

**Answer**: Yes.  Use the `task_id` argument to :meth:`Task.apply_async`::

    >>> task.apply_async(args, kwargs, task_id='…')


Can I use decorators with tasks?
--------------------------------

**Answer**: Yes.  But please see note in the sidebar at :ref:`task-basics`.

.. _faq-natural-task-ids:

Can I use natural task ids?
---------------------------

**Answer**: Yes, but make sure it is unique, as the behavior
for two tasks existing with the same id is undefined.

The world will probably not explode, but at the worst
they can overwrite each others results.

.. _faq-task-callbacks:

How can I run a task once another task has finished?
----------------------------------------------------

**Answer**: You can safely launch a task inside a task.
Also, a common pattern is to add callbacks to tasks:

.. code-block:: python

    from celery.utils.log import get_task_logger

    logger = get_task_logger(__name__)

    @app.task
    def add(x, y):
        return x + y

    @app.task(ignore_result=True)
    def log_result(result):
        logger.info("log_result got: %r", result)

Invocation::

    >>> (add.s(2, 2) | log_result.s()).delay()

See :doc:`userguide/canvas` for more information.

.. _faq-cancel-task:

Can I cancel the execution of a task?
-------------------------------------
**Answer**: Yes. Use `result.revoke`::

    >>> result = add.apply_async(args=[2, 2], countdown=120)
    >>> result.revoke()

or if you only have the task id::

    >>> from proj.celery import app
    >>> app.control.revoke(task_id)

.. _faq-node-not-receiving-broadcast-commands:

Why aren't my remote control commands received by all workers?
--------------------------------------------------------------

**Answer**: To receive broadcast remote control commands, every worker node
uses its host name to create a unique queue name to listen to,
so if you have more than one worker with the same host name, the
control commands will be received in round-robin between them.

To work around this you can explicitly set the nodename for every worker
using the :option:`-n` argument to :mod:`~celery.bin.worker`:

.. code-block:: bash

    $ celery -A proj worker -n worker1@%h
    $ celery -A proj worker -n worker2@%h

where ``%h`` is automatically expanded into the current hostname.

.. _faq-task-routing:

Can I send some tasks to only some servers?
--------------------------------------------

**Answer:** Yes. You can route tasks to an arbitrary server using AMQP,
and a worker can bind to as many queues as it wants.

See :doc:`userguide/routing` for more information.

.. _faq-change-periodic-task-interval-at-runtime:

Can I change the interval of a periodic task at runtime?
--------------------------------------------------------

**Answer**: Yes. You can use the Django database scheduler, or you can
create a new schedule subclass and override
:meth:`~celery.schedules.schedule.is_due`:

.. code-block:: python

    from celery.schedules import schedule


    class my_schedule(schedule):

        def is_due(self, last_run_at):
            return …

.. _faq-task-priorities:

Does celery support task priorities?
------------------------------------

**Answer**: No. In theory, yes, as AMQP supports priorities. However
RabbitMQ doesn't implement them yet.

The usual way to prioritize work in Celery, is to route high priority tasks
to different servers. In the real world this may actually work better than per message
priorities. You can use this in combination with rate limiting to achieve a
highly responsive system.

.. _faq-acks_late-vs-retry:

Should I use retry or acks_late?
--------------------------------

**Answer**: Depends. It's not necessarily one or the other, you may want
to use both.

`Task.retry` is used to retry tasks, notably for expected errors that
is catchable with the `try:` block. The AMQP transaction is not used
for these errors: **if the task raises an exception it is still acknowledged!**

The `acks_late` setting would be used when you need the task to be
executed again if the worker (for some reason) crashes mid-execution.
It's important to note that the worker is not known to crash, and if
it does it is usually an unrecoverable error that requires human
intervention (bug in the worker, or task code).

In an ideal world you could safely retry any task that has failed, but
this is rarely the case. Imagine the following task:

.. code-block:: python

    @app.task
    def process_upload(filename, tmpfile):
        # Increment a file count stored in a database
        increment_file_counter()
        add_file_metadata_to_db(filename, tmpfile)
        copy_file_to_destination(filename, tmpfile)

If this crashed in the middle of copying the file to its destination
the world would contain incomplete state. This is not a critical
scenario of course, but you can probably imagine something far more
sinister. So for ease of programming we have less reliability;
It's a good default, users who require it and know what they
are doing can still enable acks_late (and in the future hopefully
use manual acknowledgement).

In addition `Task.retry` has features not available in AMQP
transactions: delay between retries, max retries, etc.

So use retry for Python errors, and if your task is idempotent
combine that with `acks_late` if that level of reliability
is required.

.. _faq-schedule-at-specific-time:

Can I schedule tasks to execute at a specific time?
---------------------------------------------------

.. module:: celery.task.base

**Answer**: Yes. You can use the `eta` argument of :meth:`Task.apply_async`.

Or to schedule a periodic task at a specific time, use the
:class:`celery.schedules.crontab` schedule behavior:


.. code-block:: python

    from celery.schedules import crontab
    from celery.task import periodic_task

    @periodic_task(run_every=crontab(hour=7, minute=30, day_of_week="mon"))
    def every_monday_morning():
        print("This is run every Monday morning at 7:30")

.. _faq-safe-worker-shutdown:

How can I safely shut down the worker?
--------------------------------------

**Answer**: Use the :sig:`TERM` signal, and the worker will finish all currently
executing jobs and shut down as soon as possible. No tasks should be lost.

You should never stop :mod:`~celery.bin.worker` with the :sig:`KILL` signal
(:option:`-9`), unless you've tried :sig:`TERM` a few times and waited a few
minutes to let it get a chance to shut down.

Also make sure you kill the main worker process, not its child processes.
You can direct a kill signal to a specific child process if you know the
process is currently executing a task the worker shutdown is depending on,
but this also means that a ``WorkerLostError`` state will be set for the
task so the task will not run again.

Identifying the type of process is easier if you have installed the
``setproctitle`` module:

.. code-block:: bash

    pip install setproctitle

With this library installed you will be able to see the type of process in ps
listings, but the worker must be restarted for this to take effect.

.. seealso::

    :ref:`worker-stopping`

.. _faq-daemonizing:

How do I run the worker in the background on [platform]?
--------------------------------------------------------
**Answer**: Please see :ref:`daemonizing`.

.. _faq-django:

Django
======

.. _faq-django-database-tables:

What purpose does the database tables created by django-celery have?
--------------------------------------------------------------------

Several database tables are created by default, these relate to

* Monitoring

    When you use the django-admin monitor, the cluster state is written
    to the ``TaskState`` and ``WorkerState`` models.

* Periodic tasks

    When the database-backed schedule is used the periodic task
    schedule is taken from the ``PeriodicTask`` model, there are
    also several other helper tables (``IntervalSchedule``,
    ``CrontabSchedule``, ``PeriodicTasks``).

* Task results

    The database result backend is enabled by default when using django-celery
    (this is for historical reasons, and thus for backward compatibility).

    The results are stored in the ``TaskMeta`` and ``TaskSetMeta`` models.
    *these tables are not created if another result backend is configured*.

.. _faq-windows:

Windows
=======

.. _faq-windows-worker-embedded-beat:

The `-B` / `--beat` option to worker doesn't work?
----------------------------------------------------------------
**Answer**: That's right. Run `celery beat` and `celery worker` as separate
services instead.

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

Celery does not depend on Django anymore. To use Celery with Django you have
to use the `django-celery`_ package.

.. _`django-celery`: http://pypi.python.org/pypi/django-celery

.. _faq-is-celery-for-rabbitmq-only:

Do I have to use AMQP/RabbitMQ?
-------------------------------

**Answer**: No.

You can also use Redis, Beanstalk, CouchDB, MongoDB or an SQL database,
see `Using other queues`_.

These "virtual transports" may have limited broadcast and event functionality.
For example remote control commands only works with AMQP and Redis.

.. _`Using other queues`:
    http://ask.github.com/celery/tutorials/otherqueues.html

Redis or a database won't perform as well as
an AMQP broker. If you have strict reliability requirements you are
encouraged to use RabbitMQ or another AMQP broker. Redis/database also use
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

:mod:`~celery.bin.celeryd` is an implementation of Celery in Python. If the
language has an AMQP client, there shouldn't be much work to create a worker
in your language.  A Celery worker is just a program connecting to the broker
to process messages.

Also, there's another way to be language independent, and that is to use REST
tasks, instead of your tasks being functions, they're URLs. With this
information you can even create simple web servers that enable preloading of
code. See: `User Guide: Remote Tasks`_.

.. _`User Guide: Remote Tasks`:
    http://ask.github.com/celery/userguide/remote-tasks.html

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

celeryd is not doing anything, just hanging
--------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.
            or `Why is Task.delay/apply\* just hanging?`.

.. _faq-results-unreliable:

Task results aren't reliably returning
--------------------------------------

**Answer:** If you're using the database backend for results, and in particular
using MySQL, see `MySQL is throwing deadlock errors, what can I do?`_.

.. _faq-publish-hanging:

Why is Task.delay/apply\*/celeryd just hanging?
-----------------------------------------------

**Answer:** There is a bug in some AMQP clients that will make it hang if
it's not able to authenticate the current user, the password doesn't match or
the user does not have access to the virtual host specified. Be sure to check
your broker logs (for RabbitMQ that is :file:`/var/log/rabbitmq/rabbit.log` on
most systems), it usually contains a message describing the reason.

.. _faq-celeryd-on-freebsd:

Does it work on FreeBSD?
------------------------

**Answer:** The multiprocessing pool requires a working POSIX semaphore
implementation which isn't enabled in FreeBSD by default. You have to enable
POSIX semaphores in the kernel and manually recompile multiprocessing.

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
receiving tasks by running the following command::

    $ rabbitmqctl list_queues -p <myvhost> name messages consumers
    Listing queues ...
    celery     2891    2

This shows that there's 2891 messages waiting to be processed in the task
queue, and there are two consumers processing them.

One reason that the queue is never emptied could be that you have a stale
worker process taking the messages hostage. This could happen if celeryd
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

Watch celeryd`s log file to see if it's able to find the task, or if some
other error is happening.

.. _faq-periodic-task-does-not-run:

Why won't my periodic task run?
-------------------------------

**Answer:** See `Why won't my Task run?`_.

.. _faq-purge-the-queue:

How do I discard all waiting tasks?
------------------------------------

**Answer:** You can use celeryctl to purge all configured task queues::

        $ celeryctl purge

or programatically::

        >>> from celery.task.control import discard_all
        >>> discard_all()
        1753

If you only want to purge messages from a specific queue
you have to use the AMQP API or the :program:`camqadm` utility::

    $ camqadm queue.purge <queue name>

The number 1753 is the number of messages deleted.

You can also start :mod:`~celery.bin.celeryd` with the
:option:`--purge` argument, to purge messages when the worker starts.

.. _faq-messages-left-after-purge:

I've discarded messages, but there are still messages left in the queue?
------------------------------------------------------------------------

**Answer:** Tasks are acknowledged (removed from the queue) as soon
as they are actually executed. After the worker has received a task, it will
take some time until it is actually executed, especially if there are a lot
of tasks already waiting for execution. Messages that are not acknowledged are
held on to by the worker until it closes the connection to the broker (AMQP
server). When that connection is closed (e.g. because the worker was stopped)
the tasks will be re-sent by the broker to the next available worker (or the
same worker when it has been restarted), so to properly purge the queue of
waiting tasks you have to stop all the workers, and then discard the tasks
using :func:`~celery.task.control.discard_all`.

.. _faq-results:

Results
=======

.. _faq-get-result-by-task-id:

How do I get the result of a task if I have the ID that points there?
----------------------------------------------------------------------

**Answer**: Use `Task.AsyncResult`::

    >>> result = MyTask.AsyncResult(task_id)
    >>> result.get()

This will give you a :class:`~celery.result.BaseAsyncResult` instance
using the tasks current result backend.

If you need to specify a custom result backend you should use
:class:`celery.result.BaseAsyncResult` directly::

    >>> from celery.result import BaseAsyncResult
    >>> result = BaseAsyncResult(task_id, backend=...)
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

For the task messages you can set the :setting:`CELERY_TASK_SERIALIZER`
setting to "json" or "yaml" instead of pickle. There is
currently no alternative solution for task results (but writing a
custom result backend using JSON is a simple task)

Note that this is not just something you should be aware of with Celery, for
example also Django uses pickle for its cache client.

Can messages be encrypted?
--------------------------

**Answer**: Some AMQP brokers supports using SSL (including RabbitMQ).
You can enable this using the :setting:`BROKER_USE_SSL` setting.

It is also possible to add additional encryption and security to messages,
if you have a need for this then you should contact the :ref:`mailing-list`.

Is it safe to run :program:`celeryd` as root?
---------------------------------------------

**Answer**: No!

We're not currently aware of any security issues, but it would
be incredibly naive to assume that they don't exist, so running
the Celery services (:program:`celeryd`, :program:`celerybeat`,
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

Running :mod:`~celery.bin.celeryd` with the :option:`-E`/:option:`--events`
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

    @task(ignore_result=True)
    def mytask():
        ...

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

How can I reuse the same connection when applying tasks?
--------------------------------------------------------

**Answer**: See the :setting:`BROKER_POOL_LIMIT` setting.
The connection pool is enabled by default since version 2.5.

.. _faq-deletes-unknown-tasks:

Why do workers delete tasks from the queue if they are unable to process them?
------------------------------------------------------------------------------
**Answer**:

The worker discards unknown tasks, messages with encoding errors and messages
that doesn't contain the proper fields (as per the task message protocol).

If it did not ack (delete) them, they would be redelivered again and again
causing a loop.

There has been talk about moving these messages to a dead-letter queue,
but that has not yet been implemented.

.. _faq-execute-task-by-name:

Can I execute a task by name?
-----------------------------

**Answer**: Yes. Use :func:`celery.execute.send_task`.
You can also execute a task by name from any language
that has an AMQP client.

    >>> from celery.execute import send_task
    >>> send_task("tasks.add", args=[2, 2], kwargs={})
    <AsyncResult: 373550e8-b9a0-4666-bc61-ace01fa4f91d>

.. _faq-get-current-task-id:

How can I get the task id of the current task?
----------------------------------------------

**Answer**: The current id and more is available in the task request::

    @task
    def mytask():
        cache.set(mytask.request.id, "Running")

For more information see :ref:`task-request-info`.

.. _faq-custom-task-ids:

Can I specify a custom task_id?
-------------------------------

**Answer**: Yes.  Use the `task_id` argument to
:meth:`~celery.execute.apply_async`::

    >>> task.apply_async(args, kwargs, task_id="...")


Can I use decorators with tasks?
--------------------------------

**Answer**: Yes.  But please see note at :ref:`tasks-decorating`.

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
Also, a common pattern is to use callback tasks:

.. code-block:: python

    @task()
    def add(x, y, callback=None):
        result = x + y
        if callback:
            subtask(callback).delay(result)
        return result


    @task(ignore_result=True)
    def log_result(result, **kwargs):
        logger = log_result.get_logger(**kwargs)
        logger.info("log_result got: %s" % (result, ))

Invocation::

    >>> add.delay(2, 2, callback=log_result.subtask())

See :doc:`userguide/tasksets` for more information.

.. _faq-cancel-task:

Can I cancel the execution of a task?
-------------------------------------
**Answer**: Yes. Use `result.revoke`::

    >>> result = add.apply_async(args=[2, 2], countdown=120)
    >>> result.revoke()

or if you only have the task id::

    >>> from celery.task.control import revoke
    >>> revoke(task_id)

.. _faq-node-not-receiving-broadcast-commands:

Why aren't my remote control commands received by all workers?
--------------------------------------------------------------

**Answer**: To receive broadcast remote control commands, every worker node
uses its host name to create a unique queue name to listen to,
so if you have more than one worker with the same host name, the
control commands will be received in round-robin between them.

To work around this you can explicitly set the host name for every worker
using the :option:`--hostname` argument to :mod:`~celery.bin.celeryd`::

    $ celeryd --hostname=$(hostname).1
    $ celeryd --hostname=$(hostname).2

etc., etc...

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
override `PeriodicTask.is_due` or turn `PeriodicTask.run_every` into a
property:

.. code-block:: python

    class MyPeriodic(PeriodicTask):

        def run(self):
            # ...

        @property
        def run_every(self):
            return get_interval_from_database(...)

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
for these errors: **if the task raises an exception it is still acknowledged!**.

The `acks_late` setting would be used when you need the task to be
executed again if the worker (for some reason) crashes mid-execution.
It's important to note that the worker is not known to crash, and if
it does it is usually an unrecoverable error that requires human
intervention (bug in the worker, or task code).

In an ideal world you could safely retry any task that has failed, but
this is rarely the case. Imagine the following task:

.. code-block:: python

    @task()
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
use manual acknowledgement)

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

    from celery.task.schedules import crontab
    from celery.task import periodic_task

    @periodic_task(run_every=crontab(hour=7, minute=30, day_of_week="mon"))
    def every_monday_morning():
        print("This is run every Monday morning at 7:30")

.. _faq-safe-worker-shutdown:

How do I shut down `celeryd` safely?
--------------------------------------

**Answer**: Use the :sig:`TERM` signal, and the worker will finish all currently
executing jobs and shut down as soon as possible. No tasks should be lost.

You should never stop :mod:`~celery.bin.celeryd` with the :sig:`KILL` signal
(:option:`-9`), unless you've tried :sig:`TERM` a few times and waited a few
minutes to let it get a chance to shut down.  As if you do tasks may be
terminated mid-execution, and they will not be re-run unless you have the
`acks_late` option set (`Task.acks_late` / :setting:`CELERY_ACKS_LATE`).

.. seealso::

    :ref:`worker-stopping`

.. _faq-daemonizing:

How do I run celeryd in the background on [platform]?
-----------------------------------------------------
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

.. _faq-windows-worker-spawn-loop:

celeryd keeps spawning processes at startup
-------------------------------------------

**Answer**: This is a known issue on Windows.
You have to start celeryd with the command::

    $ python -m celeryd.bin.celeryd

Any additional arguments can be appended to this command.

See http://bit.ly/bo9RSw

.. _faq-windows-worker-embedded-beat:

The `-B` / `--beat` option to celeryd doesn't work?
----------------------------------------------------------------
**Answer**: That's right. Run `celerybeat` and `celeryd` as separate
services instead.

.. _faq-windows-django-settings:

`django-celery` can't find settings?
--------------------------------------

**Answer**: You need to specify the :option:`--settings` argument to
:program:`manage.py`::

    $ python manage.py celeryd start --settings=settings

See http://bit.ly/bo9RSw

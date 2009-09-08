============================
 Frequently Asked Questions
============================

Troubleshooting
===============

MySQL is throwing deadlock errors, what can I do?
-------------------------------------------------

**Answer:** MySQL has default isolation level set to ``REPEATABLE-READ``,
if you don't really need that, set it to ``READ-COMMITTED``.
You can do that by adding the following to your ``my.cnf``::

    [mysqld]
    transaction-isolation = READ-COMMITTED

For more information about InnoDBs transaction model see `MySQL - The InnoDB
Transaction Model and Locking`_ in the MySQL user manual.

(Thanks to Honza Kral and Anton Tsigularov for this solution)

.. _`MySQL - The InnoDB Transaction Model and Locking`: http://dev.mysql.com/doc/refman/5.1/en/innodb-transaction-model.html

celeryd is not doing anything, just hanging
--------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.
            or `Why is Task.delay/apply\* just hanging?`.

Why is Task.delay/apply\*/celeryd just hanging?
-----------------------------------------------

**Answer:** There is a bug in some AMQP clients that will make it hang if
it's not able to authenticate the current user, the password doesn't match or
the user does not have access to the virtual host specified. Be sure to check
your broker logs (for RabbitMQ that is ``/var/log/rabbitmq/rabbit.log`` on
most systems), it usually contains a message describing the reason.

Why won't celeryd run on FreeBSD?
---------------------------------

**Answer:** multiprocessing.Pool requires a working POSIX semaphore
implementation which isn't enabled in FreeBSD by default. You have to enable
POSIX semaphores in the kernel and manually recompile multiprocessing.

I'm having ``IntegrityError: Duplicate Key`` errors. Why?
----------------------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.
Thanks to howsthedotcom.

Why isn't my tasks processed?
-----------------------------
**Answer:** With RabbitMQ you can see how many consumers are currently
receiving tasks by running the following command::

    $ rabbitmqctl list_queues -p <myvhost> name messages consumers
    Listing queues ...
    celery     2891    2

This shows that there's 2891 messages waiting to be processed in the task
queue, and there are two consumers processing them.

One reason that the queue is never emptied could be that you have a stale
celery process taking the messages hostage. This could happen if celeryd
wasn't properly shut down.

When a message is recieved by a worker the broker waits for it to be
acknowledged before marking the message as processed. The broker will not
re-send that message to another consumer until the consumer is shutdown
properly.

If you hit this problem you have to kill all workers manually and restart
them::

    ps auxww | grep celeryd | awk '{print $2}' | xargs kill

You might have to wait a while until all workers has finished the work they're
doing, if it's still hanging after a long time you can kill them by force
with::

    ps auxww | grep celeryd | awk '{print $2}' | xargs kill -9

Why won't my Task run?
----------------------

**Answer:** Did you register the task in the applications ``tasks.py`` module?
(or in some other module Django loads by default, like ``models.py``?).
Also there might be syntax errors preventing the tasks module being imported.

You can find out if the celery daemon is able to run the task by executing the
task manually:

    >>> from myapp.tasks import MyPeriodicTask
    >>> MyPeriodicTask.delay()

Watch celery daemons logfile (or output if not running as a daemon), to see
if it's able to find the task, or if some other error is happening.

Why won't my Periodic Task run?
-------------------------------

**Answer:** See `Why won't my Task run?`_.

How do I discard all waiting tasks?
------------------------------------

**Answer:** Use ``celery.task.discard_all()``, like this:

    >>> from celery.task import discard_all
    >>> discard_all()
    1753

The number ``1753`` is the number of messages deleted.

You can also start celeryd with the ``--discard`` argument which will
accomplish the same thing.

I've discarded messages, but there are still messages left in the queue?
------------------------------------------------------------------------

**Answer:** Tasks are acknowledged (removed from the queue) as soon
as they are actually executed. After the worker has received a task, it will
take some time until it is actually executed, especially if there are a lot
of tasks already waiting for execution. Messages that are not acknowledged are
hold on to by the worker until it closes the connection to the broker (AMQP
server). When that connection is closed (e.g because the worker was stopped)
the tasks will be re-sent by the broker to the next available worker (or the
same worker when it has been restarted), so to properly purge the queue of
waiting tasks you have to stop all the workers, and then discard the tasks
using ``discard_all``.

Brokers
=======

Can I use celery with ActiveMQ/STOMP?
-------------------------------------

**Answer**: Yes. But this is somewhat experimental for now.
It is certainly working ok for me in a test configuration, but it has not
been tested in production like RabbitMQ. If you have any problems with
using STOMP and celery, please report the bugs to the issue tracker:

    http://github.com/ask/celery/issues/

First you have to use the ``master`` branch of ``celery``::

    $ git clone git://github.com/ask/celery.git
    $ cd celery
    $ sudo python setup.py install
    $ cd ..

Then you need to install the ``stompbackend`` branch of ``carrot``::

    $ git clone git://github.com/ask/carrot.git
    $ cd carrot
    $ git checkout stompbackend
    $ sudo python setup.py install
    $ cd ..

And my fork of ``python-stomp`` which adds non-blocking support::

    $ hg clone http://bitbucket.org/asksol/python-stomp/
    $ cd python-stomp
    $ sudo python setup.py install
    $ cd ..

In this example we will use a queue called ``celery`` which we created in
the ActiveMQ web admin interface.

**Note**: For ActiveMQ the queue name has to have ``"/queue/"`` prepended to
it. i.e. the queue ``celery`` becomes ``/queue/celery``.

Since a STOMP queue is a single named entity and it doesn't have the
routing capabilities of AMQP you need to set both the ``queue``, and
``exchange`` settings to your queue name. This is a minor inconvenience since
carrot needs to maintain the same interface for both AMQP and STOMP (obviously
the one with the most capabilities won).

Use the following specific settings in your ``settings.py``:

.. code-block:: python

    # Makes python-stomp the default backend for carrot.
    CARROT_BACKEND = "stomp"

    # STOMP hostname and port settings.
    AMQP_HOST = "localhost"
    AMQP_PORT = 61613

    # The queue name to use (both queue and exchange must be set to the
    # same queue name when using STOMP)
    CELERY_AMQP_CONSUMER_QUEUE = "/queue/celery"
    CELERY_AMQP_EXCHANGE = "/queue/celery" 
   
Now you can go on reading the tutorial in the README, ignoring any AMQP
specific options. 

Which features are not supported when using STOMP?
--------------------------------------------------

This is a (possible incomplete) list of features not available when
using the STOMP backend:

    * routing keys

    * exchange types (direct, topic, headers, etc)

    * immediate

    * mandatory

Features
========

Can I send some tasks to only some servers?
--------------------------------------------

**Answer:** As of now there is only one use-case that works like this,
and that is tasks of type ``A`` can be sent to servers ``x`` and ``y``,
while tasks of type ``B`` can be sent to server ``z``. One server can't
handle more than one routing_key, but this is coming in a later release.

Say you have two servers, ``x``, and ``y`` that handles regular tasks,
and one server ``z``, that only handles feed related tasks, you can use this
configuration:

    * Servers ``x`` and ``y``: settings.py:

    .. code-block:: python

        AMQP_SERVER = "rabbit"
        AMQP_PORT = 5678
        AMQP_USER = "myapp"
        AMQP_PASSWORD = "secret"
        AMQP_VHOST = "myapp"

        CELERY_AMQP_CONSUMER_QUEUE = "regular_tasks"
        CELERY_AMQP_EXCHANGE = "tasks"
        CELERY_AMQP_PUBLISHER_ROUTING_KEY = "task.regular"
        CELERY_AMQP_CONSUMER_ROUTING_KEY = "task.#"
        CELERY_AMQP_EXCHANGE_TYPE = "topic"

    * Server ``z``: settings.py:

    .. code-block:: python

        AMQP_SERVER = "rabbit"
        AMQP_PORT = 5678
        AMQP_USER = "myapp"
        AMQP_PASSWORD = "secret"
        AMQP_VHOST = "myapp"
        
        CELERY_AMQP_EXCHANGE = "tasks"
        CELERY_AMQP_PUBLISHER_ROUTING_KEY = "task.regular"
        CELERY_AMQP_EXCHANGE_TYPE = "topic"
        # This is the settings different for this server:
        CELERY_AMQP_CONSUMER_QUEUE = "feed_tasks"
        CELERY_AMQP_CONSUMER_ROUTING_KEY = "feed.#"

Now to make a Task run on the ``z`` server you need to set its
``routing_key`` attribute so it starts with the words ``"task.feed."``:

.. code-block:: python

    from feedaggregator.models import Feed
    from celery.task import Task

    class FeedImportTask(Task):
        routing_key = "feed.importer"

        def run(self, feed_url):
            # something importing the feed
            Feed.objects.import_feed(feed_url)


You can also override this using the ``routing_key`` argument to
:func:`celery.task.apply_async`:

    >>> from celery.task import apply_async
    >>> from myapp.tasks import RefreshFeedTask
    >>> apply_async(RefreshFeedTask, args=["http://cnn.com/rss"],
    ...             routing_key="feed.importer")


Can I use celery without Django?
--------------------------------

**Answer:** Yes.

Celery uses something called loaders to read/setup configuration, import
modules that registers tasks and to decide what happens when a task is
executed. Currently there are two loaders, the default loader and the Django
loader. If you want to use celery without a Django project, you either have to
use the default loader, or write a loader of your own.

The rest of this answer describes how to use the default loader.

First of all, installation. You need to get the development version of
celery from github::

    $ git clone git://github.com/ask/celery.git
    $ cd celery
    # python setup.py install # as root

While it is possible to use celery from outside of Django, we still need
Django itself to run, this is to use the ORM and cache-framework, etc.
Duplicating these features would be time consuming and mostly pointless, so
we decided that having a dependency on Django itself was a good thing.
Install Django using your favorite install tool, ``easy_install``, ``pip``, or
whatever::

    # easy_install django # as root

You need a configuration file named ``celeryconfig.py``, either in the
directory you run ``celeryd`` in, or in a Python library path where it is
able to find it. The configuration file can contain any of the settings
described in :mod:`celery.conf`, and in additional if you're using the
database backend you have to configure the database. Here is an example
configuration using the database backend with MySQL:

.. code-block:: python

    # Broker configuration
    AMQP_SERVER = "localhost"
    AMQP_PORT = "5672"
    AMQP_VHOST = "celery"
    AMQP_USER = "celery"
    AMQP_PASSWORD = "celerysecret"
    CARROT_BACKEND="amqp"

    # Using the database backend.
    CELERY_BACKEND = "database"
    DATABASE_ENGINE = "mysql" # see Django docs for a description of these.
    DATABASE_NAME = "mydb"
    DATABASE_HOST = "mydb.example.org"
    DATABASE_USER = "myuser"
    DATABASE_PASSWORD = "mysecret"

    # Number of processes that processes tasks simultaneously.
    CELERYD_CONCURRENCY = 8

    # Modules to import when celeryd starts.
    # This must import every module where you register tasks so celeryd
    # is able to find and run them.
    CELERY_IMPORTS = ("mytaskmodule1", "mytaskmodule2")
    
Now with this configuration file in the current directory you have to
run ``celeryinit`` to create the database tables::

    $ celeryinit

Then you should be able to successfully run ``celeryd``::

    $ celeryd --loglevel=INFO

and send a task from a python shell (note that it must be able to import
``celeryconfig.py``):

    >>> from celery.task.builtins import PingTask
    >>> result = PingTask.apply_async()
    >>> result.get()
    'pong'

The celery test-suite is failing
--------------------------------

**Answer**: You're running tests from your own Django applicaiton, and celerys
tests are failing and celerys tests are failing in that context?
If so, read on for a trick, if not please report the test failure to our issue
tracker at GitHub.
    
    http://github.com/ask/celery/issues/

That Django is running tests for all applications in ``INSTALLED_APPS``
is a pet peeve of mine. You should use a test runner that either

    1) Explicitly lists the apps you want to run tests for, or

    2) make a test runner that skips tests for apps you don't want to run.

For example this test runner that celery is using:

    http://bit.ly/NVKep

To use this add the following to your settings.py:

.. code-block:: python

    TEST_RUNNER = "celery.tests.runners.run_tests"
    TEST_APPS = (
        "app1",
        "app2",
        "app3",
        "app4",
    )

If you just want to skip celery you could use:

.. code-block:: python

    INSTALLED_APPS = (.....)
    TEST_RUNNER = "celery.tests.runners.run_tests"
    TEST_APPS = filter(lambda k: k != "celery", INSTALLED_APPS)

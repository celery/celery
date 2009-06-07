============================
 Frequently Asked Questions
============================

Questions
=========

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

I'm having ``IntegrityError: Duplicate Key`` errors. Why?
----------------------------------------------------------

**Answer:** See `MySQL is throwing deadlock errors, what can I do?`_.
Thanks to howsthedotcom.

My Periodic Tasks won't run
----------------------------

**Answer:** Did you register the task in the applications ``tasks.py`` module?
(or in some other module Django loads by default, like ``models.py``?).

You can find out if the celery daemon is able to run the task by executing the
periodic task manually, by doing the following:

    >>> from myapp.tasks import MyPeriodicTask
    >>> MyPeriodicTask.delay()

Watch celery daemons logfile (or output if not running as a daemon), to see
if it's able to find the task, or if some other error is happening.

Can I send some tasks to only some servers?
--------------------------------------------

As of now there is only one use-case that works like this, and that is
tasks of type ``A`` can be sent to servers ``x`` and ``y``, while tasks
of type ``B`` can be sent to server ``z``. One server can't handle more than
one routing_key, but this is coming in a later release.

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
        
        CELERY_AMQP_CONSUMER_QUEUE = "feed_tasks"
        CELERY_AMQP_EXCHANGE = "tasks"
        CELERY_AMQP_PUBLISHER_ROUTING_KEY = "task.regular"
        CELERY_AMQP_CONSUMER_ROUTING_KEY = "task.feed.#"
        CELERY_AMQP_EXCHANGE_TYPE = "topic"

Now to make a Task run on the ``z`` server you need to set its
``routing_key`` attribute so it starts with the words ``"task.feed."``:

.. code-block:: python

    from feedaggregator.models import Feed
    from celery.task import Task

    class FeedImportTask(Task):
        name = "import_feed"
        routing_key = "task.feed.importer"

        def run(self, feed_url):
            # something importing the feed
            Feed.objects.import_feed(feed_url)


You can also override this using the ``routing_key`` argument to
:func:`celery.task.apply_async`:

    >>> from celery.task import apply_async
    >>> from myapp.tasks import RefreshFeedTask
    >>> apply_async(RefreshFeedTask, args=["http://cnn.com/rss"],
    ...             routing_key="task.feed.importer")

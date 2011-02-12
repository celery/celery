.. _guide-routing:

===============
 Routing Tasks
===============

.. warning::

    This document refers to functionality only available in brokers
    using AMQP.  Other brokers may implement some functionality, see their
    respective documentation for more information, or contact the
    :ref:`mailing-list`.

.. contents::
    :local:


.. _routing-basics:

Basics
======

.. _routing-automatic:

Automatic routing
-----------------

The simplest way to do routing is to use the
:setting:`CELERY_CREATE_MISSING_QUEUES` setting (on by default).

With this setting on, a named queue that is not already defined in
:setting:`CELERY_QUEUES` will be created automatically.  This makes it easy to
perform simple routing tasks.

Say you have two servers, `x`, and `y` that handles regular tasks,
and one server `z`, that only handles feed related tasks.  You can use this
configuration::

    CELERY_ROUTES = {"feed.tasks.import_feed": {"queue": "feeds"}}

With this route enabled import feed tasks will be routed to the
`"feeds"` queue, while all other tasks will be routed to the default queue
(named `"celery"` for historical reasons).

Now you can start server `z` to only process the feeds queue like this::

    (z)$ celeryd -Q feeds

You can specify as many queues as you want, so you can make this server
process the default queue as well::

    (z)$ celeryd -Q feeds,celery

.. _routing-changing-default-queue:

Changing the name of the default queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can change the name of the default queue by using the following
configuration:

.. code-block:: python

    CELERY_QUEUES = {"default": {"exchange": "default",
                                 "binding_key": "default"}}
    CELERY_DEFAULT_QUEUE = "default"

.. _routing-autoqueue-details:

How the queues are defined
~~~~~~~~~~~~~~~~~~~~~~~~~~

The point with this feature is to hide the complex AMQP protocol for users
with only basic needs. However -- you may still be interested in how these queues
are declared.

A queue named `"video"` will be created with the following settings:

.. code-block:: python

    {"exchange": "video",
     "exchange_type": "direct",
     "routing_key": "video"}

The non-AMQP backends like `ghettoq` does not support exchanges, so they
require the exchange to have the same name as the queue. Using this design
ensures it will work for them as well.

.. _routing-manual:

Manual routing
--------------

Say you have two servers, `x`, and `y` that handles regular tasks,
and one server `z`, that only handles feed related tasks, you can use this
configuration:

.. code-block:: python

    CELERY_DEFAULT_QUEUE = "default"
    CELERY_QUEUES = {
        "default": {
            "binding_key": "task.#",
        },
        "feed_tasks": {
            "binding_key": "feed.#",
        },
    }
    CELERY_DEFAULT_EXCHANGE = "tasks"
    CELERY_DEFAULT_EXCHANGE_TYPE = "topic"
    CELERY_DEFAULT_ROUTING_KEY = "task.default"

:setting:`CELERY_QUEUES` is a map of queue names and their
exchange/type/binding_key, if you don't set exchange or exchange type, they
will be taken from the :setting:`CELERY_DEFAULT_EXCHANGE` and
:setting:`CELERY_DEFAULT_EXCHANGE_TYPE` settings.

To route a task to the `feed_tasks` queue, you can add an entry in the
:setting:`CELERY_ROUTES` setting:

.. code-block:: python

    CELERY_ROUTES = {
            "feeds.tasks.import_feed": {
                "queue": "feed_tasks",
                "routing_key": "feed.import",
            },
    }


You can also override this using the `routing_key` argument to
:func:`~celery.execute.apply_async`, or :func:`~celery.execute.send_task`:

    >>> from feeds.tasks import import_feed
    >>> import_feed.apply_async(args=["http://cnn.com/rss"],
    ...                         queue="feed_tasks",
    ...                         routing_key="feed.import")


To make server `z` consume from the feed queue exclusively you can
start it with the ``-Q`` option::

    (z)$ celeryd -Q feed_tasks --hostname=z.example.com

Servers `x` and `y` must be configured to consume from the default queue::

    (x)$ celeryd -Q default --hostname=x.example.com
    (y)$ celeryd -Q default --hostname=y.example.com

If you want, you can even have your feed processing worker handle regular
tasks as well, maybe in times when there's a lot of work to do::

    (z)$ celeryd -Q feed_tasks,default --hostname=z.example.com

If you have another queue but on another exchange you want to add,
just specify a custom exchange and exchange type:

.. code-block:: python

    CELERY_QUEUES = {
            "feed_tasks": {
                "binding_key": "feed.#",
            },
            "regular_tasks": {
                "binding_key": "task.#",
            },
            "image_tasks": {
                "binding_key": "image.compress",
                "exchange": "mediatasks",
                "exchange_type": "direct",
            },
        }

If you're confused about these terms, you should read up on AMQP.

.. seealso::

    In addition to the :ref:`amqp-primer` below, there's
    `Rabbits and Warrens`_, an excellent blog post describing queues and
    exchanges. There's also AMQP in 10 minutes*: `Flexible Routing Model`_,
    and `Standard Exchange Types`_. For users of RabbitMQ the `RabbitMQ FAQ`_
    could be useful as a source of information.

.. _`Rabbits and Warrens`: http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/
.. _`Flexible Routing Model`: http://bit.ly/95XFO1
.. _`Standard Exchange Types`: http://bit.ly/EEWca
.. _`RabbitMQ FAQ`: http://www.rabbitmq.com/faq.html

.. _amqp-primer:

AMQP Primer
===========

Messages
--------

A message consists of headers and a body.  Celery uses headers to store
the content type of the message and its content encoding.  The
content type is usually the serialization format used to serialize the
message. The body contains the name of the task to execute, the
task id (UUID), the arguments to execute it with and some additional
metadata -- like the number of retries or an ETA.

This is an example task message represented as a Python dictionary:

.. code-block:: python

    {"task": "myapp.tasks.add",
     "id": "54086c5e-6193-4575-8308-dbab76798756",
     "args": [4, 4],
     "kwargs": {}}

.. _amqp-producers-consumers-brokers:

Producers, consumers and brokers
--------------------------------

The client sending messages is typically called a *publisher*, or
a *producer*, while the entity receiving messages is called
a *consumer*.

The *broker* is the message server, routing messages from producers
to consumers.

You are likely to see these terms used a lot in AMQP related material.

.. _amqp-exchanges-queues-keys:

Exchanges, queues and routing keys.
-----------------------------------

1. Messages are sent to exchanges.
2. An exchange routes messages to one or more queues.  Several exchange types
   exists, providing different ways to do routing, or implementing
   different messaging scenarios.
3. The message waits in the queue until someone consumes it.
4. The message is deleted from the queue when it has been acknowledged.

The steps required to send and receive messages are:

1. Create an exchange
2. Create a queue
3. Bind the queue to the exchange.

Celery automatically creates the entities necessary for the queues in
:setting:`CELERY_QUEUES` to work (except if the queue's `auto_declare`
setting is set to :const:`False`).

Here's an example queue configuration with three queues;
One for video, one for images and one default queue for everything else:

.. code-block:: python

    CELERY_QUEUES = {
        "default": {
            "exchange": "default",
            "binding_key": "default"},
        "videos": {
            "exchange": "media",
            "binding_key": "media.video",
        },
        "images": {
            "exchange": "media",
            "binding_key": "media.image",
        }
    }
    CELERY_DEFAULT_QUEUE = "default"
    CELERY_DEFAULT_EXCHANGE_TYPE = "direct"
    CELERY_DEFAULT_ROUTING_KEY = "default"

.. note::

    In Celery the `routing_key` is the key used to send the message,
    while `binding_key` is the key the queue is bound with.  In the AMQP API
    they are both referred to as the routing key.

.. _amqp-exchange-types:

Exchange types
--------------

The exchange type defines how the messages are routed through the exchange.
The exchange types defined in the standard are `direct`, `topic`,
`fanout` and `headers`.  Also non-standard exchange types are available
as plug-ins to RabbitMQ, like the `last-value-cache plug-in`_ by Michael
Bridgen.

.. _`last-value-cache plug-in`:
    http://github.com/squaremo/rabbitmq-lvc-plugin

.. _amqp-exchange-type-direct:

Direct exchanges
~~~~~~~~~~~~~~~~

Direct exchanges match by exact routing keys, so a queue bound by
the routing key `video` only receives messages with that routing key.

.. _amqp-exchange-type-topic:

Topic exchanges
~~~~~~~~~~~~~~~

Topic exchanges matches routing keys using dot-separated words, and the
wildcard characters: ``*`` (matches a single word), and ``#`` (matches
zero or more words).

With routing keys like ``usa.news``, ``usa.weather``, ``norway.news`` and
``norway.weather``, bindings could be ``*.news`` (all news), ``usa.#`` (all
items in the USA) or ``usa.weather`` (all USA weather items).

.. _amqp-api:

Related API commands
--------------------

.. method:: exchange.declare(exchange_name, type, passive,
                             durable, auto_delete, internal)

    Declares an exchange by name.

    :keyword passive: Passive means the exchange won't be created, but you
        can use this to check if the exchange already exists.

    :keyword durable: Durable exchanges are persistent.  That is - they survive
        a broker restart.

    :keyword auto_delete: This means the queue will be deleted by the broker
        when there are no more queues using it.


.. method:: queue.declare(queue_name, passive, durable, exclusive, auto_delete)

    Declares a queue by name.

    Exclusive queues can only be consumed from by the current connection.
    Exclusive also implies `auto_delete`.

.. method:: queue.bind(queue_name, exchange_name, routing_key)

    Binds a queue to an exchange with a routing key.
    Unbound queues will not receive messages, so this is necessary.

.. method:: queue.delete(name, if_unused=False, if_empty=False)

    Deletes a queue and its binding.

.. method:: exchange.delete(name, if_unused=False)

    Deletes an exchange.

.. note::

    Declaring does not necessarily mean "create".  When you declare you
    *assert* that the entity exists and that it's operable.  There is no
    rule as to whom should initially create the exchange/queue/binding,
    whether consumer or producer.  Usually the first one to need it will
    be the one to create it.

.. _amqp-api-hands-on:

Hands-on with the API
---------------------

Celery comes with a tool called :program:`camqadm` (short for Celery AMQ Admin).
It's used for command-line access to the AMQP API, enabling access to
administration tasks like creating/deleting queues and exchanges, purging
queues or sending messages.

You can write commands directly in the arguments to :program:`camqadm`,
or just start with no arguments to start it in shell-mode::

    $ camqadm
    -> connecting to amqp://guest@localhost:5672/.
    -> connected.
    1>

Here ``1>`` is the prompt.  The number 1, is the number of commands you
have executed so far.  Type ``help`` for a list of commands available.
It also supports auto-completion, so you can start typing a command and then
hit the `tab` key to show a list of possible matches.

Let's create a queue we can send messages to::

    1> exchange.declare testexchange direct
    ok.
    2> queue.declare testqueue
    ok. queue:testqueue messages:0 consumers:0.
    3> queue.bind testqueue testexchange testkey
    ok.

This created the direct exchange ``testexchange``, and a queue
named ``testqueue``.  The queue is bound to the exchange using
the routing key ``testkey``.

From now on all messages sent to the exchange ``testexchange`` with routing
key ``testkey`` will be moved to this queue.  We can send a message by
using the ``basic.publish`` command::

    4> basic.publish "This is a message!" testexchange testkey
    ok.

Now that the message is sent we can retrieve it again.  We use the
``basic.get``` command here, which polls for new messages on the queue.

Pop a message off the queue::

    5> basic.get testqueue
    {'body': 'This is a message!',
     'delivery_info': {'delivery_tag': 1,
                       'exchange': u'testexchange',
                       'message_count': 0,
                       'redelivered': False,
                       'routing_key': u'testkey'},
     'properties': {}}


AMQP uses acknowledgment to signify that a message has been received
and processed successfully.  If the message has not been acknowledged
and consumer channel is closed, the message will be delivered to
another consumer.

Note the delivery tag listed in the structure above; Within a connection
channel, every received message has a unique delivery tag,
This tag is used to acknowledge the message.  Also note that
delivery tags are not unique across connections, so in another client
the delivery tag `1` might point to a different message than in this channel.

You can acknowledge the message we received using ``basic.ack``::

    6> basic.ack 1
    ok.

To clean up after our test session we should delete the entities we created::

    7> queue.delete testqueue
    ok. 0 messages deleted.
    8> exchange.delete testexchange
    ok.


.. _routing-tasks:

Routing Tasks
=============

.. _routing-defining-queues:

Defining queues
---------------

In Celery available queues are defined by the :setting:`CELERY_QUEUES` setting.

Here's an example queue configuration with three queues;
One for video, one for images and one default queue for everything else:

.. code-block:: python

    CELERY_QUEUES = {
        "default": {
            "exchange": "default",
            "binding_key": "default"},
        "videos": {
            "exchange": "media",
            "exchange_type": "topic",
            "binding_key": "media.video",
        },
        "images": {
            "exchange": "media",
            "exchange_type": "topic",
            "binding_key": "media.image",
        }
    }
    CELERY_DEFAULT_QUEUE = "default"
    CELERY_DEFAULT_EXCHANGE = "default"
    CELERY_DEFAULT_EXCHANGE_TYPE = "direct"
    CELERY_DEFAULT_ROUTING_KEY = "default"

Here, the :setting:`CELERY_DEFAULT_QUEUE` will be used to route tasks that
doesn't have an explicit route.

The default exchange, exchange type and routing key will be used as the
default routing values for tasks, and as the default values for entries
in :setting:`CELERY_QUEUES`.

.. _routing-task-destination:

Specifying task destination
---------------------------

The destination for a task is decided by the following (in order):

1. The :ref:`routers` defined in :setting:`CELERY_ROUTES`.
2. The routing arguments to :func:`~celery.execute.apply_async`.
3. Routing related attributes defined on the :class:`~celery.task.base.Task`
   itself.

It is considered best practice to not hard-code these settings, but rather
leave that as configuration options by using :ref:`routers`;
This is the most flexible approach, but sensible defaults can still be set
as task attributes.

.. _routers:

Routers
-------

A router is a class that decides the routing options for a task.

All you need to define a new router is to create a class with a
``route_for_task`` method:

.. code-block:: python

    class MyRouter(object):

        def route_for_task(self, task, args=None, kwargs=None):
            if task == "myapp.tasks.compress_video":
                return {"exchange": "video",
                        "exchange_type": "topic",
                        "routing_key": "video.compress"}
            return None

If you return the ``queue`` key, it will expand with the defined settings of
that queue in :setting:`CELERY_QUEUES`::

    {"queue": "video", "routing_key": "video.compress"}

    becomes -->

        {"queue": "video",
         "exchange": "video",
         "exchange_type": "topic",
         "routing_key": "video.compress"}


You install router classes by adding them to the :setting:`CELERY_ROUTES`
setting::

    CELERY_ROUTES = (MyRouter(), )

Router classes can also be added by name::

    CELERY_ROUTES = ("myapp.routers.MyRouter", )


For simple task name -> route mappings like the router example above,
you can simply drop a dict into :setting:`CELERY_ROUTES` to get the
same behavior:

.. code-block:: python

    CELERY_ROUTES = ({"myapp.tasks.compress_video": {
                            "queue": "video",
                            "routing_key": "video.compress"
                     }}, )

The routers will then be traversed in order, it will stop at the first router
returning a true value, and use that as the final route for the task.

===============
 Routing Tasks
===============

**NOTE** This document refers to functionality only available in brokers
using AMQP. Other brokers may implement some functionality, see their
respective documenation for more information, or contact the `mailinglist`_.

.. _`mailinglist`: http://groups.google.com/group/celery-users

.. contents::
    :local:

Basics
======

Automatic routing
-----------------

The simplest way to do routing is to use the ``CELERY_CREATE_MISSING_QUEUES``
setting (on by default).

When this setting is on a named queue that is not already defined in
``CELERY_QUEUES`` will be created automatically. This makes it easy to perform
simple routing tasks.

Say you have two servers, ``x``, and ``y`` that handles regular tasks,
and one server ``z``, that only handles feed related tasks, you can use this
configuration:

    CELERY_ROUTES = {"feed.tasks.import_feed": "feeds"}

With this route enabled import feed tasks will be routed to the
``"feeds"`` queue, while all other tasks will be routed to the default queue
(named ``"celery"`` for historic reasons).

Now you can start server ``z`` to only process the feeds queue like this::

    (z)$ celeryd -Q feeds

You can specify as many queues as you want, so you can make this server
process the default queue as well::

    (z)$ celeryd -Q feeds,celery

Changing the name of the default queue
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can change the name of the default queue by using the following
configuration:

.. code-block:: python

    CELERY_QUEUES = {"default": {"exchange": "default",
                                 "binding_key": "default"}}
    CELERY_DEFAULT_QUEUE = "default"

How the queues are defined
~~~~~~~~~~~~~~~~~~~~~~~~~~

The point with this feature is to hide the complex AMQP protocol for users
with only basic needs. However, you may still be interested in how these queues
are defined.

A queue named ``"video"`` will be created with the following settings:

.. code-block:: python

    {"exchange": "video",
     "exchange_type": "direct",
     "routing_key": "video"}

The non-AMQP backends like ``ghettoq`` does not support exchanges, so they
require the exchange to have the same name as the queue. Using this design
ensures it will work for them as well.

Manual routing
--------------

Say you have two servers, ``x``, and ``y`` that handles regular tasks,
and one server ``z``, that only handles feed related tasks, you can use this
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

``CELERY_QUEUES`` is a map of queue names and their exchange/type/binding_key,
if you don't set exchange or exchange type, they will be taken from the
``CELERY_DEFAULT_EXCHANGE``/``CELERY_DEFAULT_EXCHANGE_TYPE`` settings.

To route a task to the ``feed_tasks`` queue, you can add an entry in the
``CELERY_ROUTES`` setting:

.. code-block:: python

    CELERY_ROUTES = {
            "feeds.tasks.import_feed": {
                "queue": "feed_tasks",
                "routing_key": "feed.import",
            },
    }


You can also override this using the ``routing_key`` argument to
:func:`~celery.execute.apply_async`, or :func:`~celery.execute.send_task`:

    >>> from feeds.tasks import import_feed
    >>> import_feed.apply_async(args=["http://cnn.com/rss"],
    ...                         queue="feed_tasks",
    ...                         routing_key="feed.import")


To make server ``z`` consume from the feed queue exclusively you can
start it with the ``-Q`` option::

    (z)$ celeryd -Q feed_tasks --hostname=z.example.com

Servers ``x`` and ``y`` must be configured to consume from the default queue::

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
            }
            "image_tasks": {
                "binding_key": "image.compress",
                "exchange": "mediatasks",
                "exchange_type": "direct",
            },
        }

If you're confused about these terms, you should read up on AMQP concepts.

In addition to the :ref:`AMQP Primer` below, there's
`Rabbits and Warrens`_, an excellent blog post describing queues and
exchanges. There's also AMQP in 10 minutes*: `Flexible Routing Model`_,
and `Standard Exchange Types`_. For users of RabbitMQ the `RabbitMQ FAQ`_
could be useful as a source of information.

.. _`Rabbits and Warrens`: http://blogs.digitar.com/jjww/2009/01/rabbits-and-warrens/
.. _`Flexible Routing Model`: http://bit.ly/95XFO1
.. _`Standard Exchange Types`: http://bit.ly/EEWca
.. _`RabbitMQ FAQ`: http://www.rabbitmq.com/faq.html

.. _`AMQP Primer`:

AMQP Primer
===========

Messages
--------

A message consists of headers and a body. Celery uses headers to store
the content type of the message and its content encoding. In Celery the
content type is usually the serialization format used to serialize the
message, and the body contains the name of the task to execute, the
task id (UUID), the arguments to execute it with and some additional
metadata - like the number of retries and its ETA (if any).

This is an example task message represented as a Python dictionary:

.. code-block:: python

    {"task": "myapp.tasks.add",
     "id": 
     "args": [4, 4],
     "kwargs": {}}

Producers, consumers and brokers
--------------------------------

The client sending messages is typically called a *publisher*, or
a *producer*, while the entity receiving messages is called
a *consumer*.

The *broker* is the message server, routing messages from producers
to consumers.

You are likely to see these terms used a lot in AMQP related material.

Exchanges, queues and routing keys.
-----------------------------------

1. Messages are sent to exchanges.
2. An exchange routes messages to one or more queues. Several exchange types
   exists, providing different ways to do routing.
3. The message waits in the queue until someone consumes from it.
4. The message is deleted from the queue when it has been acknowledged.

The steps required to send and receive messages are:

1. Create an exchange
2. Create a queue
3. Bind the queue to the exchange.

Celery automatically creates the entities necessary for the queues in
``CELERY_QUEUES`` to work (except if the queue's ``auto_declare`` setting
is set to :const:`False`).

Here's an example queue configuration with three queues;
One for video, one for images and finally, one default queue for everything else:

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


**NOTE**: In Celery the ``routing_key`` is the key used to send the message,
while ``binding_key`` is the key the queue is bound with. In the AMQP API
they are both referred to as the routing key.

Exchange types
--------------

The exchange type defines how the messages are routed through the exchange.
The exchange types defined in the standard are ``direct``, ``topic``,
``fanout`` and ``headers``. Also non-standard exchange types are available
as plugins to RabbitMQ, like the `last-value-cache plug-in`_ by Michael
Bridgen. 

.. _`last-value-cache plug-in`:
    http://github.com/squaremo/rabbitmq-lvc-plugin

Direct exchanges
~~~~~~~~~~~~~~~~

Direct exchanges match by exact routing keys, so a queue bound with
the routing key ``video`` only receives messages with the same routing key.

Topic exchanges
~~~~~~~~~~~~~~~

Topic exchanges matches routing keys using dot-separated words, and can
include wildcard characters: ``*`` matches a single word, ``#`` matches
zero or more words.

With routing keys like ``usa.news``, ``usa.weather``, ``norway.news`` and
``norway.weather``, bindings could be ``*.news`` (all news), ``usa.#`` (all
items in the USA) or ``usa.weather`` (all USA weather items).


Related API commands
--------------------

exchange.declare(exchange_name, type, passive, durable, auto_delete, internal)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declares an exchange by name.

* ``passive`` means the exchange won't be created, but you can use this to
  check if the exchange already exists.

* Durable exchanges are persistent. That is - they survive a broker restart.

* ``auto_delete`` means the queue will be deleted by the broker when there
  are no more queues using it.

queue.declare(queue_name, passive, durable, exclusive, auto_delete)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Declares a queue by name.

* exclusive queues can only be consumed from by the current connection.
  implies ``auto_delete``.

queue.bind(queue_name, exchange_name, routing_key)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Binds a queue to an exchange with a routing key.
Unbound queues will not receive messages, so this is necessary.

queue.delete(name, if_unused, if_empty)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deletes a queue and its binding.

exchange.delete(name, if_unused)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Deletes an exchange.

**NOTE**: Declaring does not necessarily mean "create". When you declare you
*assert* that the entity exists and that it's operable. There is no rule as to
whom should initially create the exchange/queue/binding, whether consumer
or producer. Usually the first one to need it will be the one to create it.

Hands-on with the API
---------------------

Celery comes with a tool called ``camqadm`` (short for celery AMQP admin).
It's used for simple admnistration tasks like creating/deleting queues and
exchanges, purging queues and sending messages. In short it's for simple
command-line access to the AMQP API.

You can write commands directly in the arguments to ``camqadm``, or just start
with no arguments to start it in shell-mode::

    $ camqadm
    -> connecting to amqp://guest@localhost:5672/.
    -> connected.
    1>

Here ``1>`` is the prompt. The number is counting the number of commands you
have executed. Type ``help`` for a list of commands. It also has
autocompletion, so you can start typing a command and then hit the
``tab`` key to show a list of possible matches.

Now let's create a queue we can send messages to::

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
key ``testkey`` will be moved to this queue. We can send a message by
using the ``basic.publish`` command::

    4> basic.publish "This is a message!" testexchange testkey
    ok.


Now that the message is sent we can retrieve it again. We use the
``basic.get`` command here, which pops a single message off the queue,
this command is not recommended for production as it implies polling, any
real application would declare consumers instead.

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
and processed successfully. The message is sent to the next receiver
if it has not been acknowledged before the client connection is closed.

Note the delivery tag listed in the structure above; Within a connection channel,
every received message has a unique delivery tag,
This tag is used to acknowledge the message. Note that
delivery tags are not unique across connections, so in another client
the delivery tag ``1`` might point to a different message than in this channel.

You can acknowledge the message we received using ``basic.ack``::

    6> basic.ack 1
    ok.

To clean up after our test session we should delete the entities we created::

    7> queue.delete testqueue
    ok. 0 messages deleted.
    8> exchange.delete testexchange
    ok.


Routing Tasks
=============

Defining queues
---------------

In Celery the queues are defined by the ``CELERY_QUEUES`` setting.

Here's an example queue configuration with three queues;
One for video, one for images and finally, one default queue for everything else:

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

Here, the ``CELERY_DEFAULT_QUEUE`` will be used to route tasks that doesn't
have an explicit route.

The default exchange, exchange type and routing key will be used as the
default routing values for tasks, and as the default values for entries
in ``CELERY_QUEUES``.

Specifying task destination
---------------------------

The destination for a task is decided by the following (in order):

1. The :ref:`routers` defined in ``CELERY_ROUTES``.
2. The routing arguments to :func:`~celery.execute.apply_async`.
3. Routing related attributes defined on the :class:`~celery.task.base.Task` itself.

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

        def route_for_task(task, args=None, kwargs=None):
            if task == "myapp.tasks.compress_video":
                return {"exchange": "video",
                        "exchange_type": "topic",
                        "routing_key": "video.compress"}

If you return the ``queue`` key, it will expand with the defined settings of
that queue in ``CELERY_QUEUES``::

    {"queue": "video", "routing_key": "video.compress"}

    becomes -->

        {"queue": "video",
         "exchange": "video",
         "exchange_type": "topic",
         "routing_key": "video.compress"}


You install router classes by adding it to the ``CELERY_ROUTES`` setting::

    CELERY_ROUTES = (MyRouter, )

Router classes can also be added by name::

    CELERY_ROUTES = ("myapp.routers.MyRouter", )


For simple task name -> route mappings like the router example above, you can simply
drop a dict into ``CELERY_ROUTES`` to get the same result::

    CELERY_ROUTES = ({"myapp.tasks.compress_video": {
                        "queue": "video",
                        "routing_key": "video.compress"}}, )

The routers will then be traversed in order, it will stop at the first router
returning a value and use that as the final route for the task.

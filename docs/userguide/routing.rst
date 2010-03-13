===============
 Routing Tasks
===============

**NOTE** This document refers to functionality only available in brokers
using AMQP. Other brokers may implement some functionality, see their
respective documenation for more information, or contact the `mailinglist`_.

.. _`mailinglist`: http://groups.google.com/group/celery-users

AMQP Primer
===========

Messages
--------

A message consists of headers and a body. Celery uses headers to store
the content type of the message and its content encoding. In Celery the
content type is usually the serialization format used to serialize the
message, and the body contains the name of the task to execute, the
task id (UUID), the arguments to execute it with and some additional
metadata - like the number of retries and its ETA if any.

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
TODO Mindblowing one-line simple explanation here. TODO

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
``CELERY_QUEUES`` to work (unless the queue's ``auto_declare`` setting
is set)

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


**NOTE**: In Celery the ``routing_key`` is the key used to send the message,
while ``binding_key`` is the key the queue is bound with. In the AMQP API
they are both referred to as a routing key.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
